(ns ^:no-doc onyx.peer.task-lifecycle
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.schema :as os]
            [onyx.static.planning :as planning :refer [find-task]]
            [onyx.static.uuid :as uuid]
            [onyx.extensions :as extensions]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :as entry]
            [onyx.log.replica]
            [onyx.messaging.common :as mc]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.messenger-state :as ms]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.peer.constants :refer [initialize-epoch]]
            [onyx.peer.task-compile :as c]
            [onyx.peer.coordinator :as coordinator :refer [new-peer-coordinator]]
            [onyx.peer.function :as function]
            [onyx.peer.operation :as operation]
            [onyx.peer.resume-point :as res]
            ;[onyx.peer.visualization :as viz]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.protocol.task-state :as t 
             :refer [advance advanced? exec get-context get-event
                     get-input-pipeline get-lifecycle 
                     get-messenger get-output-pipeline get-replica
                     get-windows-state goto-next-batch! goto-next-iteration!
                     goto-recover! heartbeat! killed? next-epoch!
                     next-replica! new-iteration? print-state reset-event!
                     sealed? set-context! set-windows-state! set-sealed!
                     set-replica! set-coordinator!  set-messenger! set-epoch!
                     update-event!]]
            [onyx.plugin.messaging-output :as mo]
            [onyx.plugin.protocols.input :as oi]
            [onyx.plugin.protocols.output :as oo]
            [onyx.plugin.protocols.plugin :as op]
            [onyx.windowing.window-compile :as wc]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.util :refer [ms->ns]]
            [onyx.types :refer [->Results ->MonitorEvent]]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info error warn trace fatal]])
  (:import [org.agrona.concurrent IdleStrategy SleepingIdleStrategy]
           [java.util.concurrent.locks LockSupport]))

(s/defn start-lifecycle? [event start-fn]
  (let [rets (start-fn event)]
    (when-not (:start-lifecycle? rets)
      (info (:onyx.core/log-prefix event) 
            "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defn input-task? [state]
  (= :input (:onyx/type (:onyx.core/task-map (get-event state)))))

(defn output-task? [state]
  (= :output (:onyx/type (:onyx.core/task-map (get-event state)))))

(defn function-task? [state]
  (= :function (:onyx/type (:onyx.core/task-map (get-event state)))))

; (s/defn flow-retry-segments :- Event
;   [{:keys [onyx.core/task-state onyx.core/state onyx.core/messenger 
;            onyx.core/monitoring onyx.core/results] :as event} 
;   (doseq [root (:retries results)]
;     (when-let [site (peer-site task-state (:completion-id root))]
;       (emit-latency :peer-retry-segment
;                     monitoring
;                     #(extensions/internal-retry-segment messenger (:id root) site))))
;   event)

(s/defn next-iteration
  [state]
  {:post [(empty? (:onyx.core/batch (:event %)))]}
  (-> state 
      (set-context! nil)
      (reset-event!)
      (update-event! #(assoc % :onyx.core/lifecycle-id (uuid/random-uuid)))
      (advance)))

(defn prepare-batch [state] 
  (if (oo/prepare-batch (get-output-pipeline state) 
                        (get-event state) 
                        (get-replica state))
    (advance state)
    state))

(defn write-batch [state] 
  (if (oo/write-batch (get-output-pipeline state) 
                      (get-event state) 
                      (get-replica state)
                      (get-messenger state))
    (advance state)
    state))

(defn handle-exception [task-info log e lifecycle exception-action group-ch outbox-ch id job-id]
  (let [data (ex-data e)
        ;; Default to original exception if Onyx didn't wrap the original exception
        inner (or (.getCause ^Throwable e) e)]
    (if (= exception-action :restart)
      (let [msg (format "Caught exception inside task lifecycle %s. Rebooting the task." lifecycle)]
        (warn (logger/merge-error-keys inner task-info id msg)) 
        (>!! group-ch [:restart-vpeer id]))
      (let [msg (format "Handling uncaught exception thrown inside task lifecycle %s. Killing the job." lifecycle)
            entry (entry/create-log-entry :kill-job {:job job-id})]
        (warn (logger/merge-error-keys e task-info id msg))
        (extensions/write-chunk log :exception inner job-id)
        (>!! outbox-ch entry)))))

(defn input-poll-barriers [state]
  (m/poll (get-messenger state))
  (advance state))

(defn evict-dead-peers! [state timed-out-peer-ids]
  (let [replica (get-replica state)
        {:keys [onyx.core/id onyx.core/outbox-ch]} (get-event state)]
    (info "Should be killing " (vec timed-out-peer-ids))
    (run! (fn [peer-id] 
            (let [entry {:fn :leave-cluster
                         :peer-parent id
                         :args {:id peer-id
                                :group-id (get-in replica [:groups-reverse-index peer-id])}}]
              (info "Peer timed out with no heartbeats. Emitting leave cluster." entry)
              (>!! outbox-ch entry)))
          timed-out-peer-ids))
  state)

(defn dead-peer-detection! [state]
  (let [messenger (get-messenger state)
        _ (run! pub/poll-heartbeats! (m/publishers messenger))
        timed-out-subs (mapcat pub/timed-out-subscribers (m/publishers messenger))
        timed-out-pubs (sub/timed-out-publishers (m/subscriber messenger))
        timed-out (concat timed-out-subs timed-out-pubs)]
    (if-not (empty? timed-out)
      (let [{:keys [onyx.core/log-prefix onyx.core/id onyx.core/task] :as event} (get-event state)
            timeout-msg (format "%s UPSTREAM TIMED OUT %s, DOWNSTREAM TIMED OUT %s" 
                                log-prefix (vec timed-out-pubs) (vec timed-out-subs) id task)
            next-state (evict-dead-peers! state (mapv (fn strip-coordinator [src-peer-id]
                                                        (if (vector? src-peer-id)
                                                          (second src-peer-id)
                                                          src-peer-id))
                                                      timed-out))] 
        (-> timeout-msg (doto println) (doto info))
        ;; backoff for a while so we don't repeatedly write leave messages
        ;; parking here is probably not the best response
        ;; we should likely kick ourselves into an idle wait state where
        ;; we await a new replica
        (LockSupport/parkNanos (* 2000 1000000))
        next-state)
      state))
    state)

(defn offer-heartbeats [state]
  (advance (heartbeat! state)))

(defn checkpoint-input [state]
  (let [{:keys [onyx.core/job-id onyx.core/task-id 
                onyx.core/slot-id onyx.core/log 
                onyx.core/tenancy-id]} (get-event state)
        pipeline (get-input-pipeline state)
        checkpoint (oi/checkpoint pipeline)
        messenger (get-messenger state)] 
    (extensions/write-checkpoint log tenancy-id job-id (t/replica-version state) (t/epoch state)
                                 task-id slot-id :input checkpoint)
    (println "Checkpointed input" job-id (t/replica-version state) 
             (t/epoch state) task-id slot-id :input) (advance state)))

(defn checkpoint-state [state]
  (let [{:keys [onyx.core/job-id onyx.core/task-id 
                onyx.core/slot-id onyx.core/log 
                onyx.core/tenancy-id]} (get-event state)
        exported-state (->> (get-windows-state state)
                            (map (juxt ws/window-id ws/export-state))
                            (into {}))] 
    (extensions/write-checkpoint log tenancy-id job-id (t/replica-version state) 
                                 (t/epoch state) task-id slot-id :windows exported-state)
    (println "Checkpointed state" job-id (t/replica-version state) 
             (t/epoch state) task-id slot-id :windows)
    (advance state)))

(defn checkpoint-output [state]
  (let [{:keys [onyx.core/job-id onyx.core/task-id 
                onyx.core/slot-id onyx.core/log 
                onyx.core/tenancy-id]} (get-event state)] 
    ;; TODO, don't need checkpointed output, unless we have some state to checkpoint
    (extensions/write-checkpoint log tenancy-id job-id (t/replica-version state) 
                                 (t/epoch state) task-id slot-id :output true)
    (println "Checkpointed output" job-id (t/replica-version state) 
             (t/epoch state) task-id slot-id :output)
    (advance state)))


;; FIXME FIXME FIXME
;; ADD PIPELINES TO EVENT MAP
(defn seal-output! [state]
  (let [{:keys [onyx.core/job-id onyx.core/task-id
                onyx.core/slot-id onyx.core/outbox-ch]} (get-event state)
        entry (entry/create-log-entry :seal-output 
                                      {:replica-version (t/replica-version state)
                                       :epoch (t/epoch state)
                                       :job-id job-id 
                                       :task-id task-id
                                       :slot-id slot-id})]
    (info "job completed:" job-id task-id (:args entry))
    (>!! outbox-ch entry)))

(defn completed? [state]
  (if (input-task? state)
    (oi/completed? (get-input-pipeline state))
    (sub/completed? (m/subscriber (get-messenger state)))))

(defn try-seal-job! [state]
  (if (and (completed? state)
           (not (sealed? state)))
    (let [_ (when (output-task? state)
              (seal-output! state))
          messenger (get-messenger state)
          {:keys [onyx.core/triggers]} (get-event state)]
      (if (empty? triggers)
        (set-sealed! state true)
        (set-sealed! (ws/assign-windows state :job-completed) true)))
    state))

(defn mark-snapshot-checkpointed! [state]
  (if (input-task? state)
    (when-let [cp-epoch (sub/checkpointed-epoch (m/subscriber (get-messenger state)))] 
      (oi/checkpointed! (get-input-pipeline state) cp-epoch))))

(defn prepare-barrier-sync [state]
  (let [messenger (get-messenger state)
        subscriber (m/subscriber messenger)]
    (if (sub/blocked? subscriber)
      (if (or (not (input-task? state))
              ;; if not synced, don't advance and unblock
              (oi/synced? (get-input-pipeline state) 
                          (inc (t/epoch state))))
        (let [state (next-epoch! state)
              _ (mark-snapshot-checkpointed! state)
              completed? (completed? state)]
          (-> state
              (try-seal-job!)
              (set-context! {:barrier-opts {:cp-epoch (sub/checkpointed-epoch subscriber)
                                            :completed? completed?}
                             :src-peers (sub/src-peers subscriber)
                             :publishers (m/publishers messenger)})
              (advance)))
        state)
      (goto-next-batch! state))))

(defn offer-barriers [state]
  (let [messenger (get-messenger state)
        {:keys [barrier-opts publishers] :as context} (get-context state)
        _ (assert (not (empty? publishers)))
        offer-xf (comp (map (fn [pub]
                              [(m/offer-barrier messenger pub barrier-opts) 
                               pub]))
                       (remove (comp pos? first))
                       (map second))
        remaining-pubs (sequence offer-xf publishers)] 
    (if (empty? remaining-pubs)
      (advance state)
      (set-context! state (assoc context :publishers remaining-pubs)))))

(defn offer-barrier-status [state]
  (let [messenger (get-messenger state)
        {:keys [src-peers] :as context} (get-context state)
        _ (assert (not (empty? src-peers)) (get-replica state))
        offer-xf (comp (map (fn [src-peer-id]
                              [(sub/offer-barrier-status! (m/subscriber messenger) src-peer-id)
                               src-peer-id]))
                       (remove (comp pos? first))
                       (map second))
        remaining-peers (sequence offer-xf src-peers)] 
    (if (empty? remaining-peers)
      (advance state)
      (set-context! state (assoc context :src-peers remaining-peers)))))

(defn unblock-subscribers [state]
  (sub/unblock! (m/subscriber (get-messenger state)))
  (advance (set-context! state nil)))

(defn seal-barriers? [state]
  (if (sub/blocked? (m/subscriber (get-messenger state)))
    (advance state)
    (goto-next-batch! state)))

(defn seal-barriers [state]
  (let [pipeline (get-output-pipeline state)]
    (if (oo/synced? pipeline (t/epoch state))
      (let [subscriber (m/subscriber (get-messenger state))]
        (when-let [cp-epoch (sub/checkpointed-epoch subscriber)]
          (oo/checkpointed! pipeline cp-epoch))
        (-> state
            (next-epoch!)
            (try-seal-job!)
            (set-context! {:src-peers (sub/src-peers subscriber)})
            (advance)))
      ;; block until synced
      state)))

;; Re-enable to prevent CPU burn?
; (defn backoff-when-drained! [event]
;   (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(defn assign-windows [state]
  (advance (ws/assign-windows state :new-segment)))

(defn build-lifecycle-invoke-fn [event lifecycle-kw]
  (if-let [f (lc/compile-lifecycle-functions event lifecycle-kw)]
    (fn [state]
      (advance (update-event! state f)))))

(defn recover-input [state]
  (let [{:keys [recover-coordinates recovered?] :as context} (get-context state)
        input-pipeline (get-input-pipeline state)]
    (when-not recovered? 
      (let [event (get-event state)
            stored (res/recover-input event recover-coordinates)
            _ (info (:onyx.core/log-prefix event) "RECOVER pipeline checkpoint" stored)]
        (oi/recover! input-pipeline (t/replica-version state) stored)))
    (if-let [synced? (oi/synced? input-pipeline (t/epoch state))] 
      (-> state
          (set-context! nil)
          (advance))
      (set-context! state (assoc context :recovered? true)))))

(defn recover-state
  [state]
  (let [{:keys [onyx.core/log-prefix 
                onyx.core/windows onyx.core/triggers
                onyx.core/task-id onyx.core/job-id onyx.core/peer-opts
                onyx.core/resume-point] :as event} (get-event state)
        {:keys [recover-coordinates]} (get-context state)
        recovered-windows (res/recover-windows event recover-coordinates)]
    (-> state 
        (set-windows-state! recovered-windows)
        ;; Notify triggers that we have recovered our windows
        (ws/assign-windows :recovered)
        (advance))))

(defn poll-recover-input-function [state]
  (let [messenger (get-messenger state)
        subscriber (m/subscriber messenger)
        _ (sub/poll! subscriber)]
    (if (and (sub/blocked? subscriber)
             (sub/recovered? subscriber)) 
        (-> state
            (next-epoch!)
            (set-context! {:recover-coordinates (sub/get-recover subscriber)
                           :barrier-opts {:recover-coordinates (sub/get-recover subscriber)
                                          :completed? false}
                           :src-peers (sub/src-peers subscriber)
                           :publishers (m/publishers messenger)})
            (advance))
      state)))

(defn poll-recover-output [state]
  (let [messenger (get-messenger state)
        subscriber (m/subscriber messenger)
        _ (sub/poll! subscriber)] 
    (if (and (sub/blocked? subscriber)
             (sub/recovered? subscriber))
      (-> state
          (next-epoch!)
          (set-context! {:recover-coordinates (sub/get-recover subscriber)
                         :src-peers (sub/src-peers subscriber)})
          (advance))
      state)))

(def DEBUG false)

(defn iteration [state-machine replica]
  ;(when DEBUG (viz/update-monitoring! state-machine))
  (loop [state (if-not (= (get-replica state-machine) replica)
                 (next-replica! state-machine replica)
                 state-machine)]
    (let [next-state (exec state)]
      (when (zero? (rand-int 10000)) 
        (print-state next-state))
      (if (and (advanced? next-state) 
               (not (new-iteration? next-state)))
        (recur next-state)
        next-state))))

(defn run-task-lifecycle!
  "The main task run loop, read batch, ack messages, etc."
  [state-machine handle-exception-fn exception-action-fn]
  (try
    (let [{:keys [onyx.core/replica-atom]} (get-event state-machine)]
      (loop [sm state-machine 
             replica-val @replica-atom]
        (debug "New task iteration:" (:onyx/type (:onyx.core/task-map (get-event sm))))
        (let [next-sm (iteration sm replica-val)]
          (if-not (killed? next-sm)
            (recur next-sm @replica-atom)
            (do
             (println "Fell out of task lifecycle loop")
             next-sm)))))
    (catch Throwable e
      (let [lifecycle (get-lifecycle state-machine)
            action (if (:kill-job? (ex-data e))
                     :kill
                     (exception-action-fn (get-event state-machine) lifecycle e))] 
        (handle-exception-fn lifecycle action e))
      state-machine)))

(defn instantiate-plugin [{:keys [onyx.core/task-map] :as event}]
  (let [kw (:onyx/plugin task-map)] 
    (case (:onyx/language task-map)
      :java (operation/instantiate-plugin-instance (name kw) event)
      (let [user-ns (namespace kw)
            user-fn (name kw)
            pipeline (if (and user-ns user-fn)
                       (if-let [f (ns-resolve (symbol user-ns) (symbol user-fn))]
                         (f event)))]
        pipeline))))

(defrecord TaskInformation 
  [log job-id task-id workflow catalog task flow-conditions windows triggers lifecycles metadata]
  component/Lifecycle
  (start [component]
    (let [catalog (extensions/read-chunk log :catalog job-id)
          task (extensions/read-chunk log :task job-id task-id)
          flow-conditions (extensions/read-chunk log :flow-conditions job-id)
          windows (extensions/read-chunk log :windows job-id)
          triggers (extensions/read-chunk log :triggers job-id)
          workflow (extensions/read-chunk log :workflow job-id)
          lifecycles (extensions/read-chunk log :lifecycles job-id)
          metadata (extensions/read-chunk log :job-metadata job-id)
          resume-point (extensions/read-chunk log :resume-point job-id task-id)]
      (assoc component 
             :workflow workflow :catalog catalog :task task :flow-conditions flow-conditions 
             :windows windows :triggers triggers :lifecycles lifecycles 
             :metadata metadata :resume-point resume-point)))
  (stop [component]
    (assoc component 
           :workflow nil :catalog nil :task nil :flow-conditions nil :windows nil 
           :triggers nil :lifecycles nil :metadata nil :resume-point nil)))

(defn new-task-information [peer task]
  (map->TaskInformation (select-keys (merge peer task) [:log :job-id :task-id :id])))

(defn filter-task-lifecycles 
  [{:keys [onyx.core/task-map onyx.core/windows onyx.core/triggers] :as event}]
  (let [task-type (:onyx/type task-map)
        windowed? (or (not (empty? windows))
                      (not (empty? triggers)))] 
    (cond-> []
      (#{:input} task-type)                   (conj {:lifecycle :lifecycle/poll-recover-input
                                                     :fn poll-recover-input-function
                                                     :blockable? true})
      (#{:function} task-type)                (conj {:lifecycle :lifecycle/poll-recover-function
                                                     :fn poll-recover-input-function
                                                     :blockable? true})
      (#{:output} task-type)                  (conj {:lifecycle :lifecycle/poll-recover-output
                                                     :fn poll-recover-output
                                                     :blockable? true})
      (#{:input :function} task-type)         (conj {:lifecycle :lifecycle/offer-barriers
                                                     :fn offer-barriers
                                                     :blockable? true})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/offer-barrier-status
                                                     :fn offer-barrier-status
                                                     :blockable? true})
      (#{:input} task-type)                   (conj {:lifecycle :lifecycle/recover-input 
                                                     :fn recover-input})
      windowed?                               (conj {:lifecycle :lifecycle/recover-state 
                                                     :fn recover-state})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/unblock-subscribers
                                                     :fn unblock-subscribers})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/next-iteration
                                                     :fn next-iteration})
      (#{:input} task-type)                   (conj {:lifecycle :lifecycle/input-poll-barriers
                                                     :fn input-poll-barriers})
      (#{:input :function} task-type)         (conj {:lifecycle :lifecycle/prepare-barrier-sync
                                                     :fn prepare-barrier-sync})
      (#{:output} task-type)                  (conj {:lifecycle :lifecycle/seal-barriers?
                                                     :fn seal-barriers?
                                                     :blockable? false})
      (#{:output} task-type)                  (conj {:lifecycle :lifecycle/seal-barriers
                                                     :fn seal-barriers
                                                     :blockable? true})
      ;; TODO: double check that checkpoint doesn't occur immediately after recovery
      (#{:input} task-type)                   (conj {:lifecycle :lifecycle/checkpoint-input
                                                     :fn checkpoint-input
                                                     :blockable? true})
      windowed?                               (conj {:lifecycle :lifecycle/checkpoint-state
                                                     :fn checkpoint-state
                                                     :blockable? true})
      ;; OUTPUT IS TOO AHEAD?
      (#{:output} task-type)                  (conj {:lifecycle :lifecycle/checkpoint-output
                                                     :fn checkpoint-output
                                                     :blockable? true})
      (#{:input :function} task-type)         (conj {:lifecycle :lifecycle/offer-barriers
                                                     :fn offer-barriers
                                                     :blockable? true})
      ;; rename aligneds -> aligments
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/offer-barrier-status
                                                     :fn offer-barrier-status
                                                     :blockable? true})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/unblock-subscribers
                                                     :fn unblock-subscribers})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/before-batch
                                                     :fn (build-lifecycle-invoke-fn event :lifecycle/before-batch)})
      (#{:input} task-type)                   (conj {:lifecycle :lifecycle/read-batch
                                                     :fn function/read-input-batch})
      (#{:function :output} task-type)        (conj {:lifecycle :lifecycle/read-batch
                                                     :fn function/read-function-batch})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/after-read-batch
                                                     :fn (build-lifecycle-invoke-fn event :lifecycle/after-read-batch)})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/apply-fn
                                                     :fn apply-fn})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/after-apply-fn
                                                     :fn (build-lifecycle-invoke-fn event :lifecycle/after-apply-fn)})

      windowed?                               (conj {:lifecycle :lifecycle/assign-windows
                                                     :fn assign-windows})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/prepare-batch
                                                     :fn prepare-batch})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/write-batch
                                                     :fn write-batch
                                                     :blockable? true})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/after-batch
                                                     :fn (build-lifecycle-invoke-fn event :lifecycle/after-batch)})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/offer-heartbeats
                                                     :fn offer-heartbeats}))))

(defn send-heartbeats! [state]
  (let [messenger (get-messenger state)]
    (run! pub/offer-heartbeat! (m/publishers messenger))
    (sub/offer-heartbeat! (m/subscriber messenger))
    state))

(deftype TaskStateMachine [^IdleStrategy idle-strategy
                           ^int recover-idx 
                           ^int iteration-idx 
                           ^int batch-idx
                           ^int nstates 
                           lifecycle-names
                           #^"[Lclojure.lang.IFn;" lifecycle-fns 
                           ^:unsynchronized-mutable ^int idx 
                           ^:unsynchronized-mutable ^java.lang.Boolean advanced 
                           ^:unsynchronized-mutable sealed
                           ^:unsynchronized-mutable replica 
                           ^:unsynchronized-mutable messenger 
                           messenger-group
                           ^:unsynchronized-mutable coordinator
                           input-pipeline
                           output-pipeline
                           init-event 
                           ^:unsynchronized-mutable event
                           ^:unsynchronized-mutable windows-state
                           ^:unsynchronized-mutable context
                           ^:unsynchronized-mutable replica-version
                           ^:unsynchronized-mutable epoch
                           heartbeat-ns
                           ^:unsynchronized-mutable last-heartbeat]
  t/PTaskStateMachine
  (start [this] this)
  (stop [this scheduler-event]
    (when coordinator (coordinator/stop coordinator scheduler-event))
    (when messenger (component/stop messenger))
    (when input-pipeline (op/stop input-pipeline event))
    (when output-pipeline (op/stop output-pipeline event))
    this)
  (killed? [this]
    (or @(:onyx.core/task-kill-flag event) @(:onyx.core/kill-flag event)))
  (new-iteration? [this]
    (= idx iteration-idx))
  (advanced? [this]
    advanced)
  (get-lifecycle [this]
    (get lifecycle-names idx))
  (heartbeat! [this]
    ;; TODO, publisher should be smarter about sending a message only when it hasn't 
    ;; been sending messages, as segments count as heartbeats too
    (let [curr-time (System/nanoTime)] 
      (if (> curr-time (+ last-heartbeat heartbeat-ns))
        (do (set! last-heartbeat curr-time)
            (-> this 
                (send-heartbeats!)
                (dead-peer-detection!)))
        this)))
  (print-state [this]
    (let [task-map (:onyx.core/task-map event)] 
      (info "Task state" 
               [(:onyx/type task-map)
                (:onyx/name task-map)
                :slot
                (:onyx.core/slot-id event)
                :id
                (:onyx.core/id event)
                (get-lifecycle this)
                :adv? advanced
                :rv
                replica-version
                :e
                epoch
                :n-pubs
                (count (m/publishers messenger))
                :batch
                (:onyx.core/batch event)
                ;:segments-gen
                ;(:segments (:onyx.core/results event))
                ]))
    this)
  (set-context! [this new-context]
    (set! context new-context)
    this)
  (get-context [this]
    context)
  (set-sealed! [this new-sealed]
    (set! sealed new-sealed)
    this)
  (sealed? [this]
    sealed)
  (get-input-pipeline [this]
    input-pipeline)
  (get-output-pipeline [this]
    output-pipeline)
  (next-replica! [this new-replica]
    (let [job-id (:onyx.core/job-id event)
          old-version (get-in replica [:allocation-version job-id])
          new-version (get-in new-replica [:allocation-version job-id])]
      (if (or (= old-version new-version)
              ;; wait for re-allocation
              (killed? this)
              (not= job-id 
                    (:job (common/peer->allocated-job (:allocations new-replica) 
                                                      (:onyx.core/id event)))))
        this
        (let [next-messenger (ms/next-messenger-state! messenger event replica new-replica)
              _ (assert next-messenger)
              next-coordinator (coordinator/next-state coordinator replica new-replica)]
          (-> this
              (set-sealed! false)
              (set-messenger! next-messenger)
              (set-coordinator! next-coordinator)
              (set-replica! new-replica)
              (goto-recover!))))))
  (set-windows-state! [this new-windows-state]
    (set! windows-state new-windows-state)
    this)
  (get-windows-state [this]
    windows-state)
  (set-replica! [this new-replica]
    (assert messenger)
    (set! replica new-replica)
    (set! replica-version (get-in new-replica [:allocation-version (:onyx.core/job-id event)]))
    (set-epoch! this initialize-epoch))
  (get-replica [this]
    replica)
  (set-event! [this new-event]
    (set! event new-event)
    this)
  (reset-event! [this]
    (set! event init-event)
    this)
  (update-event! [this f]
    (set! event (f event))
    this)
  (get-event [this] event)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    (m/set-epoch! messenger new-epoch)
    this)
  (next-epoch! [this]
    (set-epoch! this (inc epoch)))
  (epoch [this]
    epoch)
  (replica-version [this]
    replica-version)
  (set-messenger! [this new-messenger]
    (set! messenger new-messenger)
    this)
  (get-messenger [this]
    messenger)
  (set-coordinator! [this next-coordinator]
    (set! coordinator next-coordinator)
    this)
  (goto-recover! [this]
    (set! idx recover-idx)
    (-> this 
        (set-context! nil)
        (reset-event!)))
  (goto-next-iteration! [this]
    (set! idx iteration-idx))
  (goto-next-batch! [this]
    (set! advanced true)
    (set! idx batch-idx)
    this)
  (get-coordinator [this]
    coordinator)
  (exec [this]
    (set! advanced false)
    (let [task-fn (aget lifecycle-fns idx)
          next-state (task-fn this)]
      (if-not advanced
        ;; TODO, have some measure of the amount of work done
        ;; For example, if we managed to send some messages
        ;; but we are still blocked, then maybe we don't want to idle
        (do (.idle idle-strategy)
            ;; FIXME: only do this every heartbeat-ms
            (heartbeat! next-state))
        next-state)))
  (advance [this]
    (let [new-idx ^int (unchecked-add-int idx 1)]
      (set! advanced true)
      (if (= new-idx nstates)
        (goto-next-iteration! this)
        (set! idx new-idx))
      this)))

(defn lookup-lifecycle-idx [lifecycles name]
  (->> lifecycles
       (map-indexed (fn [idx v]
                      (if (= name (:lifecycle v))
                        idx)))
       (remove nil?)
       (first)))

(defn lookup-batch-start-endex [lifecycles]
  ;; before-batch may be stripped, thus before or read may be first batch fn
  (int (or (lookup-lifecycle-idx lifecycles :lifecycle/before-batch)
           (lookup-lifecycle-idx lifecycles :lifecycle/read-batch))))

(defn new-state-machine [event opts messenger messenger-group coordinator] 
  (let [{:keys [onyx.core/input-plugin onyx.core/output-plugin]} event
        {:keys [replica-version] :as base-replica} (onyx.log.replica/starting-replica opts)
        lifecycles (filter :fn (filter-task-lifecycles event))
        names (mapv :lifecycle lifecycles)
        arr #^"[Lclojure.lang.IFn;" (into-array clojure.lang.IFn (map :fn lifecycles))
        recover-idx (int 0)
        iteration-idx (int (lookup-lifecycle-idx lifecycles :lifecycle/next-iteration))
        batch-idx (lookup-batch-start-endex lifecycles)
        heartbeat-ns (ms->ns (arg-or-default :onyx.peer/heartbeat-ms opts))
        idle-strategy (SleepingIdleStrategy. (arg-or-default :onyx.peer/idle-sleep-ns opts))
        window-states (c/event->windows-states event)]
    (->TaskStateMachine idle-strategy recover-idx iteration-idx batch-idx (alength arr) names arr 
                        (int 0) false false base-replica messenger messenger-group coordinator 
                        input-plugin output-plugin event event window-states nil
                        replica-version initialize-epoch heartbeat-ns (System/nanoTime))))

;; NOTE: currently, if task doesn't start before the liveness timeout, the peer will be killed
;; this should be re-evaluated
(defn backoff-until-task-start! 
  [{:keys [onyx.core/kill-flag onyx.core/task-kill-flag onyx.core/opts] :as event} start-fn]
  (while (and (not (or @kill-flag @task-kill-flag))
              (not (start-lifecycle? event start-fn)))
    (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts))))

(defn start-task-lifecycle! [state handle-exception-fn exception-action-fn]
  (thread (run-task-lifecycle! state handle-exception-fn exception-action-fn)))

(defn take-final-state!! [component]
  (<!! (:task-lifecycle-ch component)))

(defn compile-task [{:keys [task-information job-id task-id id monitoring log
                            replica-origin replica opts outbox-ch group-ch task-kill-flag kill-flag]}]
  (let [{:keys [workflow catalog task flow-conditions resume-point 
                windows triggers lifecycles metadata]} task-information
        log-prefix (logger/log-prefix task-information)
        task-map (find-task catalog (:name task))
        filtered-windows (vec (wc/filter-windows windows (:name task)))
        window-ids (set (map :window/id filtered-windows))
        filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
        _ (info log-prefix "Compiling lifecycle")]
    (assert (:onyx/tenancy-id opts) (:onyx/tenancy-id opts))
    (->> {:onyx.core/id id
          :onyx.core/tenancy-id (:onyx/tenancy-id opts)
          :onyx.core/job-id job-id
          :onyx.core/task-id task-id
          :onyx.core/slot-id (get-in replica-origin [:task-slot-ids job-id task-id id])
          :onyx.core/task (:name task)
          :onyx.core/catalog catalog
          :onyx.core/workflow workflow
          :onyx.core/windows filtered-windows
          :onyx.core/triggers filtered-triggers
          :onyx.core/flow-conditions flow-conditions
          :onyx.core/lifecycles lifecycles
          :onyx.core/metadata metadata
          :onyx.core/task-map task-map
          :onyx.core/serialized-task task
          :onyx.core/log log
          :onyx.core/monitoring monitoring
          :onyx.core/task-information task-information
          :onyx.core/outbox-ch outbox-ch
          :onyx.core/group-ch group-ch
          :onyx.core/task-kill-flag task-kill-flag
          :onyx.core/kill-flag kill-flag
          :onyx.core/peer-opts opts
          :onyx.core/fn (operation/resolve-task-fn task-map)
          :onyx.core/resume-point resume-point
          :onyx.core/replica-atom replica
          :onyx.core/log-prefix log-prefix}
         c/task-params->event-map
         c/flow-conditions->event-map
         c/task->event-map)))

(defn build-input-pipeline [{:keys [onyx.core/task-map] :as event}]
  (if (= :input (:onyx/type task-map))
    (op/start (instantiate-plugin event) event)))

(defn build-output-pipeline [{:keys [onyx.core/task-map] :as event}]
  (if (= :output (:onyx/type task-map))
    (op/start (instantiate-plugin event) event)
    (op/start (mo/->MessengerOutput nil) event)))

(defrecord TaskLifeCycle
  [id log messenger messenger-group job-id task-id replica group-ch log-prefix
   kill-flag outbox-ch completion-ch peer-group opts task-kill-flag
   scheduler-event task-monitoring task-information replica-origin]

  component/Lifecycle
  (start [component]
    (let [handle-exception-fn (fn [lifecycle action e]
                                 (handle-exception task-information log e lifecycle 
                                                   action group-ch outbox-ch id job-id))]
      (try
       (let [log-prefix (logger/log-prefix task-information)
             event (compile-task component)
             exception-action-fn (lc/compile-lifecycle-handle-exception-functions event)
             start?-fn (lc/compile-start-task-functions event)
             before-task-start-fn (or (lc/compile-lifecycle-functions event :lifecycle/before-task-start) 
                                      identity)
             after-task-stop-fn (or (lc/compile-lifecycle-functions event :lifecycle/after-task-stop) 
                                    identity)]
         (try
          (info log-prefix "Warming up task lifecycle" (:onyx.core/serialized-task event))
          (backoff-until-task-start! event start?-fn)
          (try 
           (let [{:keys [onyx.core/task-map] :as event} (before-task-start-fn event)]
             (try
              (let [input-pipeline (build-input-pipeline event)
                    output-pipeline (build-output-pipeline event)
                    coordinator (new-peer-coordinator (:workflow task-information)
                                                      (:resume-point task-information)
                                                      log messenger-group 
                                                      opts id job-id group-ch)
                    event (-> event
                              (assoc :onyx.core/input-plugin input-pipeline)
                              (assoc :onyx.core/output-plugin output-pipeline))
                    state (new-state-machine event opts messenger messenger-group coordinator)
                    _ (info log-prefix "Enough peers are active, starting the task")
                    task-lifecycle-ch (start-task-lifecycle! state handle-exception-fn exception-action-fn)]
                (s/validate os/Event event)
                (assoc component
                       :event event
                       :state state
                       :log-prefix log-prefix
                       :task-information task-information
                       :after-task-stop-fn after-task-stop-fn
                       :task-kill-flag task-kill-flag
                       :kill-flag kill-flag
                       :task-lifecycle-ch task-lifecycle-ch
                       ;; atom for storing peer test state in property test
                       :holder (atom nil)))
              (catch Throwable e
                (let [lifecycle :lifecycle/initializing
                      action (exception-action-fn event lifecycle e)]
                  (handle-exception-fn lifecycle action e)
                  component))))
           (catch Throwable e
             (let [lifecycle :lifecycle/before-task-start
                   action (exception-action-fn event lifecycle e)]
               (handle-exception-fn lifecycle action e)) 
             component))
          (catch Throwable e
            (let [lifecycle :lifecycle/start-task?
                  action (exception-action-fn event lifecycle e)]
              (handle-exception-fn lifecycle action e))
            component)))
       (catch Throwable e
         ;; kill job as errors are unrecoverable if thrown in the compile stage
         (handle-exception-fn :lifecycle/compiling :kill e)
         component))))

  (stop [component]
    (if-let [task-name (:name (:task (:task-information component)))]
      (info (:log-prefix component) "Stopping task lifecycle.")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up."))

    (when-let [event (:event component)]
      (debug (:log-prefix component) "Stopped task. Waiting to fall out of task loop.")
      (reset! (:kill-flag component) true)
      (when-let [final-state (take-final-state!! component)]
        (t/stop final-state (:scheduler-event component))
        (reset! (:task-kill-flag component) true))
      (when-let [f (:after-task-stop-fn component)] 
        ;; do we want after-task-stop to fire before seal / job completion, at
        ;; the risk of it firing more than once?
        ;; we may need an extra lifecycle function which can be used for job completion, 
        ;; but not cleaning up resources
        (f event)))
    (assoc component
           :event nil
           :state nil
           :holder nil
           :log-prefix nil
           :task-information nil
           :task-kill-flag nil
           :kill-flag nil
           :task-lifecycle-ch nil)))

(defn task-lifecycle [peer task]
  (map->TaskLifeCycle (merge peer task)))
