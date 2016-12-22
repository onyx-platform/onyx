(ns ^:no-doc onyx.peer.task-lifecycle
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [debug info error warn trace fatal]]
            [schema.core :as s]
            [onyx.schema :as os]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :as entry]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.static.planning :as planning :refer [find-task]]
            [onyx.static.uuid :as uuid]
            [onyx.protocol.task-state :refer :all]
            [onyx.peer.coordinator :as coordinator :refer [new-peer-coordinator]]
            [onyx.windowing.window-compile :as wc]
            [onyx.peer.task-compile :as c]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.peer.function :as function]
            [onyx.peer.operation :as operation]
            [onyx.plugin.messaging-output :as mo]
            [onyx.compression.nippy :refer [messaging-decompress]]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.peer.visualization :as viz]
            [onyx.messaging.messenger-state :as ms]
            [onyx.log.replica]
            [onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent]]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :refer [apply-fn]]
            [onyx.plugin.protocols.input :as oi]
            [onyx.plugin.protocols.output :as oo]
            [onyx.plugin.protocols.plugin :as op]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.static.logging :as logger]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.messaging.common :as mc]))

(s/defn start-lifecycle? [event start-fn]
  (let [rets (start-fn event)]
    (when-not (:start-lifecycle? rets)
      (info (:log-prefix event) "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defrecord SegmentRetries [segments retries])

(defn add-from-leaf 
  [event result root leaves accum {:keys [message] :as leaf}]
  (let [routes (r/route-data event result message)
        message* (r/flow-conditions-transform message routes event)
        leaf* (if (= message message*)
                leaf
                (assoc leaf :message message*))]
    (if (= :retry (:action routes))
      (assoc accum :retries (conj! (:retries accum) root))
      (update accum :segments (fn [s] (conj! s (assoc leaf* :flow (:flow routes))))))))

;; TODO REMOVE
(s/defn add-from-leaves
  "Flattens root/leaves and accumulates new segments and retries"
  [segments retries event :- os/Event result]
  (let [root (:root result)
        leaves (:leaves result)]
    (reduce (fn [accum leaf]
              (add-from-leaf event result root leaves accum leaf))
            (->SegmentRetries segments retries)
            leaves)))

;; Get rid of this stuff
(defn persistent-results! [results]
  (->Results (:tree results)
             (persistent! (:segments results))
             (persistent! (:retries results))))

(defn build-new-segments [state]
  (let [{:keys [onyx.core/results onyx.core/monitoring] :as event} (get-event state)]
    (-> state
        (set-event! (emit-latency 
                     :peer-batch-latency 
                     monitoring
                     #(let [results (reduce (fn [accumulated result]
                                              (let [root (:root result)
                                                    segments (:segments accumulated)
                                                    retries (:retries accumulated)
                                                    ret (add-from-leaves segments retries event result)]
                                                (->Results (:tree results) (:segments ret) (:retries ret))))
                                            results
                                            (:tree results))]
                        (assoc event :onyx.core/results (persistent-results! results)))))
        (advance)))) 

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
  {:post [(empty? (:onyx.core/batch (:event %)))
          (empty? (:segments (:onyx.core/results (:event %))))]}
  (-> state 
      (set-context! nil)
      (init-event!)
      (set-event! (assoc (get-event state) :onyx.core/lifecycle-id (uuid/random-uuid)))
      (advance)))

(defn prepare-batch [state] 
  (let [[advance? pipeline ev] (oo/prepare-batch (get-output-pipeline state) 
                                                 (get-event state) 
                                                 (get-replica state))]
    (cond-> (set-output-pipeline! state pipeline)
      (not-empty ev) (set-event! (merge (get-event state) ev))
      advance? (advance))))

(defn write-batch [state] 
  (let [[advance? pipeline ev] (oo/write-batch (get-output-pipeline state) 
                                               (get-event state) 
                                               (get-replica state)
                                               (get-messenger state))]
    (cond-> (set-output-pipeline! state pipeline)
      (not-empty ev) (set-event! (merge (get-event state) ev))
      advance? (advance))))

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

(defn do-poll-heartbeats! [state]
  (run! pub/poll-heartbeats! (m/publishers (get-messenger state)))
  state)

(defn poll-heartbeats [state]
  (advance (do-poll-heartbeats! state)))

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

(defn dead-peer-detection [state]
  ;; TODO; should only do this every n ms
  (if (zero? (rand-int 500))
    (let [_ (do-poll-heartbeats! state)
          messenger (get-messenger state)
          timed-out-subs (mapcat pub/timed-out-subscribers (m/publishers messenger))
          timed-out-pubs (sub/timed-out-publishers (m/subscriber messenger))
          timed-out (concat timed-out-subs timed-out-pubs)]
      (if-not (empty? timed-out)
        (do (println "UPSTREAM TIMED OUT" timed-out-pubs
                     "DOWNSTREAM TIMED OUT" timed-out-subs
                     "detected from" 
                     (:onyx.core/id (get-event state)) 
                     (:onyx.core/task (get-event state)))
            (-> state 
                (evict-dead-peers! (map (fn strip-coordinator [src-peer-id]
                                          (if (vector? src-peer-id)
                                            (second src-peer-id)
                                            src-peer-id))
                                        timed-out))
                (goto-recover!)))
        (advance state)))
    (advance state)))

(defn offer-heartbeats [state]
  (advance (heartbeat! state)))

(defn checkpoint-input [state]
  (let [messenger (get-messenger state)
        {:keys [onyx.core/job-id onyx.core/task-id 
                onyx.core/slot-id onyx.core/log]} (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)
        pipeline (get-input-pipeline state)
        checkpoint (oi/checkpoint pipeline)] 
    (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id :input checkpoint)
    (println "Checkpointed input" job-id replica-version epoch task-id slot-id :input)
    (advance state)))

(defn checkpoint-state [state]
  (let [messenger (get-messenger state)
        {:keys [onyx.core/job-id onyx.core/task-id onyx.core/slot-id onyx.core/log]} (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)] 
    (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id 
                                 :state 
                                 (mapv ws/export-state (get-windows-state state)))
    (println "Checkpointed state" job-id replica-version epoch task-id slot-id :state)
    (advance state)))

(defn checkpoint-output [state]
  (let [messenger (get-messenger state)
        {:keys [onyx.core/job-id onyx.core/task-id 
                onyx.core/slot-id onyx.core/log]} (get-event state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)] 
    (extensions/write-checkpoint log job-id replica-version epoch task-id slot-id 
                                 :output true)
    (println "Checkpointed output" job-id replica-version epoch task-id slot-id :output)
    (advance state)))

;; Figure out barrier sync issue
; (defn pipeline-barrier-sync [state]
;   (let [messenger (get-messenger state)
;         [advanced? pipeline] (oi/synced? (get-input-pipeline state) (m/epoch messenger))] 
;     (-> state 
;         (set-input-pipeline! pipeline)
;         )
;     (cond-> 
;       advanced? (advance))))

(defn prepare-barrier-sync [state]
  (let [messenger (get-messenger state)] 
    (if (m/barriers-aligned? messenger)
      (let [_ (m/next-epoch! messenger)
            e (m/epoch messenger)
            input? (= :input (:onyx/type (:onyx.core/task-map (get-event state))))
            pipeline (cond-> (get-input-pipeline state)
                       input? (oi/synced? e)
                       true second)
            completed? (if input? ;; use a different interface? then don't need to switch on this
                         (oi/completed? pipeline)
                         (m/all-barriers-completed? messenger))]
        (-> state 
            (set-input-pipeline! pipeline)
            (set-context! {:barrier-opts {:completed? completed?}
                           :src-peers (sub/src-peers (m/subscriber messenger))
                           :publishers (m/publishers messenger)})
            (advance)))
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
        _ (assert (not (empty? src-peers)))
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
  (m/unblock-subscriber! (get-messenger state))
  (advance (set-context! state nil)))

(defn complete-job! [state]
  (let [messenger (get-messenger state)
        {:keys [onyx.core/job-id onyx.core/task-id 
                onyx.core/slot-id onyx.core/outbox-ch]} (get-event state)
        entry (entry/create-log-entry :seal-output 
                                      {:replica-version (m/replica-version messenger)
                                       :job-id job-id 
                                       :task-id task-id
                                       :slot-id slot-id})]
    (info "job completed:" job-id task-id (:args entry))
    (>!! outbox-ch entry)
    (set-sealed! state true)))

(defn seal-barriers? [state]
  (if (m/barriers-aligned? (get-messenger state))
    (advance state)
    (goto-next-batch! state)))

(defn seal-barriers [state]
  (let [messenger (get-messenger state)
        [advance? pipeline] (oo/synced? (get-output-pipeline state) (m/epoch messenger))] 
    (set-output-pipeline! state pipeline)
    (if advance?
      (do
       (when (and (m/all-barriers-completed? messenger) 
                  (not (sealed? state)))
         (complete-job! state))
       (m/next-epoch! messenger)
       (-> state
           ;; prepare to send barrier status
           (set-context! {:src-peers (sub/src-peers (m/subscriber messenger))})
           (advance)))
      ;; block until synchronised
      state)))

;; Re-enable to prevent CPU burn?
; (defn backoff-when-drained! [event]
;   (Thread/sleep (arg-or-default :onyx.peer/drained-back-off (:peer-opts event))))

(defn assign-windows [state]
  (ws/assign-windows state :new-segment))

(defn build-lifecycle-invoke-fn [event lifecycle-kw]
  (let [f (lc/compile-lifecycle-functions event lifecycle-kw)] 
    (if f
      (fn [state]
        (advance (set-event! state (f (get-event state))))))))

(defn read-checkpoint
  [{:keys [onyx.core/log onyx.core/job-id onyx.core/task-id onyx.core/slot-id] :as event} 
   checkpoint-type 
   recover]
  (println "RECOVER IS " recover)
  (if-not (= :beginning recover)
    (extensions/read-checkpoint log job-id (first recover) (second recover) 
                                task-id slot-id checkpoint-type)))

(defn recover-input [state]
  (let [{:keys [recover] :as context} (get-context state)
        _ (assert recover)
        event (get-event state)
        stored (read-checkpoint event :input recover)
        messenger (get-messenger state)
        replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)
        _ (info "RECOVER pipeline checkpoint" (:onyx.core/job-id event)
                (:onyx.core/task-id event) stored)
        next-pipeline (-> state
                          (get-input-pipeline)
                          (oi/recover replica-version stored)
                          (oi/synced? epoch)
                          ;; HMM
                          second)]
    (-> state
        (set-input-pipeline! next-pipeline)
        (advance))))

(defn recover-state
  [state]
  (let [{:keys [onyx.core/log-prefix onyx.core/task-map 
                onyx.core/windows onyx.core/triggers] :as event} (get-event state)
        {:keys [recover] :as context} (get-context state)
        _ (assert recover)
        stored (read-checkpoint event :state recover)
        recovered-windows (->> windows
                               (mapv (fn [window] (wc/resolve-window-state window triggers task-map)))
                               (mapv (fn [stored ws]
                                       (if stored
                                         (let [recovered (ws/recover-state ws stored)] 
                                           (info "RECOVER state" (:onyx.core/id event) stored)
                                           recovered) 
                                         ws))
                                     (or stored (repeat nil))))]
    (-> state 
        (set-windows-state! recovered-windows)
        ;; Notify triggers that we have recovered our windows
        ;; ws/assign-windows will auto advance
        (ws/assign-windows :recovered))))

(defn poll-recover-input-function [state]
  (let [messenger (get-messenger state)] 
    (if-let [recover (m/poll-recover messenger)]
      (do
       (m/next-epoch! messenger)
       (-> state
           (set-context! {:recover recover
                          :barrier-opts {:recover recover 
                                         :completed? false}
                          :src-peers (sub/src-peers (m/subscriber messenger))
                          :publishers (m/publishers messenger)})
           (advance)))
      state)))

(defn poll-recover-output [state]
  (let [messenger (get-messenger state)] 
    (if-let [recover (m/poll-recover messenger)]
      (do
       (m/next-epoch! messenger)
       (-> state
           (set-context! {:src-peers (sub/src-peers (m/subscriber messenger))})
           (advance)))
      state)))

(def DEBUG false)

(defn iteration [state-machine replica]
  (when DEBUG (viz/update-monitoring! state-machine))
  (loop [state (if-not (= (get-replica state-machine) replica)
                 (next-replica! state-machine replica)
                 state-machine)]
    (let [next-state (exec state)]
      (when (zero? (rand-int 1000)) 
        (print-state next-state))
      (cond (and (advanced? next-state) 
                 (not (new-iteration? next-state)))
            (recur next-state)

            (new-iteration? next-state)
            next-state

            :else ;; blocked
            (do
             (.sleep java.util.concurrent.TimeUnit/NANOSECONDS 500000)
             (debug "Polling heartbeats because blocked, keep things going")
             ;; Probably shouldn't always offer heartbeats
             ;; here because we are technically blocked?
             (-> next-state 
                 (do-poll-heartbeats!)
                 (heartbeat!)))))))

(defn run-task-lifecycle
  "The main task run loop, read batch, ack messages, etc."
  [state-machine ex-f]
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
     (ex-f e state-machine)
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
          metadata (extensions/read-chunk log :job-metadata job-id)]
      (assoc component 
             :workflow workflow :catalog catalog :task task :flow-conditions flow-conditions
             :windows windows :triggers triggers :lifecycles lifecycles :metadata metadata)))
  (stop [component]
    (assoc component 
           :catalog nil :task nil :flow-conditions nil :windows nil 
           :triggers nil :lifecycles nil :metadata nil)))

(defn new-task-information [peer task]
  (map->TaskInformation (select-keys (merge peer task) [:log :job-id :task-id :id])))

;; Publisher heartbeats
;; At the start of each cycle, set all publications to used? false
;; Whenever you send a message / barrier / whatever to a publication, set used? true
;; At the end of the cycle, send heartbeats to used? false publications
;; Make all of this done by the messenger

;; Subscriber heartbeats
;; At the end of each cycle, check whether subscriber is still alive
;; If it is not, do not send a message and consider re-establishing via log message / peer reboot
;; If it is, then send a heartbeat in the backchannel to the publisher

;; TODO, try to filter out lifecycles that are actually identity
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
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/poll-heartbeats
                                                     :fn poll-heartbeats})
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
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/dead-peer-detection
                                                     :fn dead-peer-detection})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/after-read-batch
                                                     :fn (build-lifecycle-invoke-fn event :lifecycle/after-read-batch)})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/apply-fn
                                                     :fn apply-fn})
      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/after-apply-fn
                                                     :fn (build-lifecycle-invoke-fn event :lifecycle/after-apply-fn)})

      (#{:input :function :output} task-type) (conj {:lifecycle :lifecycle/build-new-segments
                                                     :fn build-new-segments})
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

(deftype TaskStateMachine [^int recover-idx 
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
                           ^:unsynchronized-mutable input-pipeline
                           ^:unsynchronized-mutable output-pipeline
                           ^:unsynchronized-mutable init-event 
                           ^:unsynchronized-mutable event
                           ^:unsynchronized-mutable windows-state
                           ^:unsynchronized-mutable context
                           heartbeat-ms
                           ^:unsynchronized-mutable last-heartbeat]
  PTaskStateMachine
  (start [this] this)
  (stop [this]
    (when coordinator (coordinator/stop coordinator))
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
    (let [curr-time (System/currentTimeMillis)] 
      (when (> curr-time (+ last-heartbeat heartbeat-ms))
        (set! last-heartbeat curr-time)
        (run! pub/offer-heartbeat! (m/publishers messenger))
        (sub/offer-heartbeat! (m/subscriber messenger)))
      this))
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
                (m/replica-version messenger)
                :e
                (m/epoch messenger)
                ; Replace with expected from subscriber itself
                ;:n-subs
                ;(count (m/subscriber messenger))
                :n-pubs
                (count (m/publishers messenger))
                :batch
                (:onyx.core/batch event)
                :segments-gen
                (:segments (:onyx.core/results event))]))
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
  (set-input-pipeline! [this new-input-pipeline]
    (set! input-pipeline new-input-pipeline)
    this)
  (set-output-pipeline! [this new-output-pipeline]
    (set! output-pipeline new-output-pipeline)
    this)
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
              next-coordinator (coordinator/next-state coordinator replica new-replica)]
          (-> this
              (set-sealed! false)
              (set-coordinator! next-coordinator)
              (set-messenger! next-messenger)
              (set-replica! new-replica)
              (goto-recover!))))))
  (set-windows-state! [this new-windows-state]
    (set! windows-state new-windows-state)
    this)
  (get-windows-state [this]
    windows-state)
  (set-replica! [this new-replica]
    (set! replica new-replica)
    this)
  (get-replica [this]
    replica)
  (init-event! [this]
    (set! event init-event)
    this)
  (set-event! [this new-event]
    (set! event new-event)
    this)
  (get-event [this] event)
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
        (init-event!)))
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
    (let [task-fn (aget lifecycle-fns idx)]
      (task-fn this)))
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

(defn new-state-machine [event opts messenger messenger-group coordinator input-pipeline output-pipeline]
  (let [base-replica (onyx.log.replica/starting-replica opts)
        lifecycles (filter :fn (filter-task-lifecycles event))
        names (mapv :lifecycle lifecycles)
        arr #^"[Lclojure.lang.IFn;" (into-array clojure.lang.IFn (map :fn lifecycles))
        recover-idx (int 0)
        iteration-idx (int (lookup-lifecycle-idx lifecycles :lifecycle/next-iteration))
        batch-idx (int ;; before-batch may be stripped
                       (or (lookup-lifecycle-idx lifecycles :lifecycle/before-batch)
                           (lookup-lifecycle-idx lifecycles :lifecycle/read-batch)))
        heartbeat-ms (arg-or-default :onyx.peer/heartbeat-ms opts)]
    (->TaskStateMachine recover-idx iteration-idx batch-idx (alength arr) names arr 
                        (int 0) false false base-replica messenger messenger-group coordinator 
                        input-pipeline output-pipeline event event (c/event->windows-states event) nil
                        heartbeat-ms (System/currentTimeMillis))))

(defn backoff-until-task-start! 
  [{:keys [onyx.core/kill-flag onyx.core/task-kill-flag onyx.core/opts] :as event} start-fn]
  (while (and (not (or @kill-flag @task-kill-flag))
              (not (start-lifecycle? event start-fn)))
    (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts))))

(defn start-task-lifecycle! [state ex-f]
  (thread (run-task-lifecycle state ex-f)))

(defn final-state [component]
  (<!! (:task-lifecycle-ch component)))

(defn compile-task [{:keys [task-information job-id task-id id monitoring log
                            replica-origin replica opts outbox-ch group-ch task-kill-flag kill-flag]}]
  (let [{:keys [workflow catalog task flow-conditions 
                windows triggers lifecycles metadata]} task-information
        log-prefix (logger/log-prefix task-information)
        task-map (find-task catalog (:name task))
        filtered-windows (vec (wc/filter-windows windows (:name task)))
        window-ids (set (map :window/id filtered-windows))
        filtered-triggers (filterv #(window-ids (:trigger/window-id %)) triggers)
        _ (info log-prefix "Compiling lifecycle")]
    (->> {:onyx.core/id id
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
          :onyx.core/replica-atom replica
          :onyx.core/log-prefix log-prefix
          ;; TODO, remove
          :messenger-slot-id (common/messenger-slot-id replica-origin job-id task-id id)}
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
    (assert (zero? (count (m/publishers messenger))))
    (assert (nil? (m/subscriber messenger)))
    (let [handle-exception-fn (fn [lifecycle action e]
                                 (handle-exception task-information log e lifecycle action group-ch outbox-ch id job-id))]
      (try
       (let [event (compile-task component)
             exception-action-fn (lc/compile-lifecycle-handle-exception-functions event)
             start?-fn (lc/compile-start-task-functions event)
             before-task-start-fn (or (lc/compile-lifecycle-functions event :lifecycle/before-task-start) 
                                      identity)
             after-task-stop-fn (or (lc/compile-lifecycle-functions event :lifecycle/after-task-stop) 
                                    identity)
             ex-f (fn [e state] 
                    (let [lifecycle (get-lifecycle state)
                          _ (assert state)
                          action (exception-action-fn (get-event state) lifecycle e)] 
                      (handle-exception-fn lifecycle action e)))]
         (try
          (info log-prefix "Warming up task lifecycle" (:onyx.core/serialized-task event))
          (backoff-until-task-start! event start?-fn)
          (try 
           (let [{:keys [onyx.core/task-map] :as event} (before-task-start-fn event)]
             (try
              (let [input-pipeline (build-input-pipeline event)
                    output-pipeline (build-output-pipeline event)
                    workflow-depth (planning/workflow-depth (:workflow task-information))
                    coordinator (new-peer-coordinator workflow-depth log messenger-group 
                                                      opts id job-id group-ch)
                    initial-state (new-state-machine event opts messenger messenger-group
                                                     coordinator input-pipeline output-pipeline)
                    _ (info log-prefix "Enough peers are active, starting the task")
                    task-lifecycle-ch (start-task-lifecycle! initial-state ex-f)]
                (s/validate os/Event event)
                (assoc component
                       :event event
                       :state initial-state
                       :log-prefix log-prefix
                       :task-information task-information
                       :after-task-stop-fn after-task-stop-fn
                       ;; atom for storing peer test state in property test
                       :holder (atom nil)
                       :task-kill-flag task-kill-flag
                       :kill-flag kill-flag
                       :task-lifecycle-ch task-lifecycle-ch))
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
      (info (:log-prefix component) "Stopping task lifecycle")
      (warn (:log-prefix component) "Stopping task lifecycle, failed to initialize task set up"))

    (when-let [event (:event component)]
      (debug (:log-prefix component) "Stopped task. Waiting to fall out")
      (reset! (:kill-flag component) true)
      (when-let [last-state (final-state component)]
        (stop last-state)
        (when-not (empty? (:onyx.core/triggers (get-event last-state)))
          (ws/assign-windows last-state (:scheduler-event component)))
        (reset! (:task-kill-flag component) true))
      (when-let [f (:after-task-stop-fn component)] 
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
