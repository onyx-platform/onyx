(ns ^:no-doc onyx.peer.task-lifecycle
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [onyx.static.planning :as planning :refer [find-task]]
            [onyx.static.uuid :as uuid]
            [onyx.extensions :as extensions]
            [onyx.checkpoint :as cp]
            [onyx.compression.nippy :refer [checkpoint-compress checkpoint-decompress]]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.information-model]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :as entry]
            [onyx.log.replica]
            [onyx.messaging.common :as mc]
            [onyx.messaging.messenger-state :as ms]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.monitoring.metrics-monitoring :as metrics-monitoring :refer [update-timer-ns!]]
            [onyx.peer.grouping :as g]
            [onyx.peer.constants :refer [initialize-epoch]]
            [onyx.peer.task-compile :as c]
            [onyx.peer.coordinator :as coordinator :refer [new-peer-coordinator]]
            [onyx.peer.read-batch :as read-batch]
            [onyx.peer.operation :as operation]
            [onyx.peer.resume-point :as res]
            [onyx.peer.liveness :refer [upstream-timed-out-peers downstream-timed-out-peers]]
            [onyx.peer.status :as status]
            [onyx.peer.window-state :as ws]
            [onyx.peer.transform :as transform :refer [apply-fn]]
            [onyx.state.protocol.db :as db]
            [onyx.protocol.task-state :as t
             :refer [advance advanced? exec get-context get-event
                     get-input-pipeline get-lifecycle evict-peer! get-state-store
                     set-state-store! get-messenger get-output-pipeline get-replica
                     get-windows-state goto-next-batch! goto-next-iteration!
                     goto-recover! heartbeat! killed? next-epoch!
                     next-replica! new-iteration? log-state reset-event!
                     sealed? set-context! set-windows-state! set-sealed!
                     set-replica! set-coordinator! set-messenger! set-epoch! 
                     initial-sync-backoff update-event! 
                     set-watermark-flag! watermark-flag?]]
            [onyx.plugin.messaging-output :as mo]
            [onyx.plugin.protocols :as p]
            [onyx.windowing.window-compile :as wc]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.logging :as logger]
            [onyx.state.serializers.utils]
            [onyx.state.serializers.checkpoint :as cpenc]
            [onyx.static.util :refer [ns->ms ms->ns deserializable-exception]]
            [onyx.types :refer [->Results ->MonitorEvent ->MonitorEventLatency]]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info error warn trace fatal]])
  (:import [org.agrona.concurrent IdleStrategy SleepingIdleStrategy BackoffIdleStrategy]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent.atomic AtomicLong AtomicInteger]
           [java.util.concurrent.locks LockSupport]))

(s/defn start-lifecycle? [event start-fn]
  (let [rets (start-fn event)]
    (when-not (:start-lifecycle? rets)
      (info (:onyx.core/log-prefix event)
            "Peer chose not to start the task yet. Backing off and retrying..."))
    rets))

(defn input-task? [event]
  (= :input (:onyx/type (:onyx.core/task-map event))))

(defn output-task? [event]
  (= :output (:onyx/type (:onyx.core/task-map event))))

(defn function-task? [event]
  (= :function (:onyx/type (:onyx.core/task-map event))))

(defn fixed-npeers? [event]
  (or (:onyx/n-peers (:onyx.core/task-map event))
      (= 1 (:onyx/max-peers (:onyx.core/task-map event)))
      (= (:onyx/min-peers (:onyx.core/task-map event))
         (:onyx/max-peers (:onyx.core/task-map event)))))

(defn windowed-task? [{:keys [onyx.core/windows onyx.core/triggers] :as event}]
  (or (not (empty? windows))
      (not (empty? triggers))))

(defn completed? [state]
  (sub/completed? (m/subscriber (get-messenger state))))

(defn checkpoint? [state]
  (sub/checkpoint? (m/subscriber (get-messenger state))))

(defn all-heartbeat-times [messenger]
  (let [downstream (->> (mapcat vals (map pub/statuses (m/publishers messenger)))
                        (map :heartbeat))
        upstream (->> (sub/status-pubs (m/subscriber messenger))
                      (vals)
                      (map status-pub/get-heartbeat))]
    (into downstream upstream)))

(defn set-received-heartbeats! [messenger monitoring]
  (let [received-timer ^com.codahale.metrics.Timer (:since-received-heartbeat monitoring)
        curr-time (System/nanoTime)]
    (run! (fn [hb]
            (.update received-timer (- curr-time hb) TimeUnit/NANOSECONDS))
          (all-heartbeat-times messenger))))

(s/defn next-iteration
  [state]
  {:post [(empty? (:onyx.core/batch (:event %)))]}
  (-> state
      (set-context! nil)
      (reset-event!)
      (update-event! #(assoc % :onyx.core/lifecycle-id (uuid/random-uuid)))
      (t/seal-checkpoints!)
      (advance)))

(defn prepare-batch [state]
  (if (p/prepare-batch (get-output-pipeline state)
                       (get-event state)
                       (get-replica state)
                       (get-messenger state))
    (advance state)
    state))

(defn write-batch [state]
  (if (p/write-batch (get-output-pipeline state)
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
        (extensions/write-chunk log :exception (deserializable-exception inner {}) job-id)
        (>!! outbox-ch entry)))))

(defn merge-statuses [state]
  (->> (get-messenger state)
       (m/publishers)
       (mapcat (comp endpoint-status/statuses pub/endpoint-status))
       (map val)
       (into [{:ready? true
               :replica-version (t/replica-version state)
               :checkpointing? (not (cp/complete? (:onyx.core/storage (get-event state))))
               :epoch (t/epoch state)
               :heartbeat (System/nanoTime)
               :min-epoch (t/epoch state)}])
       status/merge-statuses))

(defn input-poll-barriers [state]
  (m/poll (get-messenger state))
  (advance state))

(defn coordinator-peer-id->peer-id [peer-id]
  (cond-> peer-id
    (vector? peer-id) second))

(defn offer-heartbeats [state]
  (advance (heartbeat! state)))

(defn check-checkpointable! [state job-id task-id]
  (when-not (fixed-npeers? (get-event state))
    (throw (ex-info (str "Task is not checkpointable, as the task onyx/n-peers is not set "
                         "and :onyx/min-peers is not equal to :onyx/max-peers.")
                    {:job-id job-id
                     :task task-id}))))

(defn checkpoint-input [state]
  (if (checkpoint? state) 
    (let [{:keys [onyx.core/job-id onyx.core/task-id onyx.core/slot-id
                  onyx.core/storage onyx.core/monitoring onyx.core/tenancy-id]} (get-event state)
          pipeline (get-input-pipeline state)
          checkpoint (p/checkpoint pipeline)
          rv (t/replica-version state)
          e (t/epoch state)
          start (System/nanoTime)
          checkpoint-bytes (checkpoint-compress checkpoint)]
      (update-timer-ns! (:checkpoint-serialization-latency monitoring) (- (System/nanoTime) start))
      (.set ^AtomicLong (:checkpoint-size monitoring) (alength checkpoint-bytes))
      (when-not (nil? checkpoint) 
        (check-checkpointable! state job-id task-id))
      (cp/write-checkpoint storage tenancy-id job-id rv e task-id slot-id :input checkpoint-bytes)
      (debug "Checkpointed input" job-id rv e task-id slot-id :input)
      (advance state))
    (advance state)))

(defn checkpoint-state [state]
  (if (checkpoint? state) 
    (let [{:keys [onyx.core/job-id onyx.core/task-id 
                  onyx.core/slot-id onyx.core/storage 
                  onyx.core/monitoring onyx.core/tenancy-id] :as event} (get-event state)
          checkpoint-encoder (cpenc/empty-checkpoint-encoder)
          metadata-bs (checkpoint-compress {:state-indexes (ws/state-indices event)})
          _ (cpenc/set-schema-version checkpoint-encoder 0)
          _ (cpenc/set-metadata checkpoint-encoder metadata-bs)
          _ (db/export (get-state-store state) checkpoint-encoder) 
          rv (t/replica-version state)
          e (t/epoch state)
          start (System/nanoTime)
          checkpoint-bytes ^bytes (cpenc/encoded-bytes checkpoint-encoder)]
      (update-timer-ns! (:checkpoint-serialization-latency monitoring) (- (System/nanoTime) start))
      (.set ^AtomicLong (:checkpoint-size monitoring) (alength checkpoint-bytes))
      (check-checkpointable! state job-id task-id)
      (cp/write-checkpoint storage tenancy-id job-id rv e task-id slot-id :windows checkpoint-bytes)
      (debug "Checkpointed state" job-id rv e task-id slot-id :windows)
      (advance state))
    (advance state)))

(defn checkpoint-output [state]
  (if (checkpoint? state) 
    (let [{:keys [onyx.core/job-id onyx.core/task-id onyx.core/slot-id
                  onyx.core/storage onyx.core/monitoring onyx.core/tenancy-id]} (get-event state)
          pipeline (get-output-pipeline state)
          checkpoint (p/checkpoint pipeline)
          rv (t/replica-version state)
          e (t/epoch state)
          start (System/nanoTime)
          checkpoint-bytes (checkpoint-compress checkpoint)]
      (update-timer-ns! (:checkpoint-serialization-latency monitoring) (- (System/nanoTime) start))
      (.set ^AtomicLong (:checkpoint-size monitoring) (alength checkpoint-bytes))
      (when-not (nil? checkpoint) 
        (check-checkpointable! state job-id task-id))
      (cp/write-checkpoint storage tenancy-id job-id rv e task-id slot-id :output checkpoint-bytes)
      (debug "Checkpointed output" job-id rv e task-id slot-id :output)
      (advance state))
    (advance state)))

(defn try-seal-job! [state]
  (if (and (completed? state)
           (not (sealed? state)))
    (let [messenger (get-messenger state)
          {:keys [onyx.core/triggers] :as event} (get-event state)]
      (cond-> state
        (not (empty? triggers))
        (ws/assign-windows (onyx.types/new-state-event :job-completed event))
        ;; we can seal with the current epoch as we are sure there will be no more data.
        true (t/seal-checkpoints! (t/replica-version state) (t/epoch state))
        true (set-sealed! true)))
    state))

(defn synced? [state]
  (cond (input-task? (get-event state))
        (p/synced? (get-input-pipeline state) (t/epoch state))

        (output-task? (get-event state))
        (p/synced? (get-output-pipeline state) (t/epoch state))

        :else true))

(defn state->watermarks [state]
  (cond-> (sub/watermarks (m/subscriber (get-messenger state)))
    (input-task? (get-event state))
    (assoc :input (t/get-watermark state))))

(defn input-function-seal-barriers? [state]
  (let [messenger (get-messenger state)
        subscriber (m/subscriber messenger)]
    (if (sub/blocked? subscriber)
      (if (synced? state)
        (let [event (get-event state)
              watermarks (state->watermarks state)
              monitoring (:onyx.core/monitoring (get-event state))]
          (.set ^AtomicLong (:workflow-watermark monitoring) (:input watermarks))
          (.set ^AtomicLong (:coordinator-watermark monitoring) (:coordinator watermarks))
          (-> state
              (next-epoch!)
              (try-seal-job!)
              (set-context! {:barrier-opts {:completed? (completed? state)
                                            :checkpoint? (checkpoint? state)
                                            :watermarks watermarks}
                             :src-peers (sub/src-peers subscriber)
                             :publishers (m/publishers messenger)})
              (set-watermark-flag! true)
              (advance)))
        ;; we need to wait until we're synced
        state)
      (goto-next-batch! state))))

(defn output-seal-barriers? [state]
  (let [subscriber (m/subscriber (get-messenger state))] 
    (if (sub/blocked? subscriber)
      (if (synced? state)
        (-> state
            (next-epoch!)
            (try-seal-job!)   
            (set-context! {:src-peers (sub/src-peers subscriber)})
            (set-watermark-flag! true)
            (advance))
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
       (-> state 
           (initial-sync-backoff)
           (set-context! (assoc context :publishers remaining-pubs))))))

(defn barrier-status-opts [state]
  (let [status (merge-statuses state)]
    {:checkpointing? (:checkpointing? status)
     :min-epoch (:min-epoch status)
     :drained? (and (or (nil? (get-input-pipeline state)) 
                        (p/completed? (get-input-pipeline state)))
                    (or (nil? (get-output-pipeline state)) 
                        (p/completed? (get-output-pipeline state))))}))

(defn offer-barrier-status [state]
  (let [messenger (get-messenger state)
        {:keys [src-peers] :as context} (get-context state)
        _ (assert (not (empty? src-peers)) (get-replica state))
        opts (assoc (barrier-status-opts state) :event :next-barrier)
        offer-xf (comp (map (fn [src-peer-id]
                              [(sub/offer-barrier-status! (m/subscriber messenger) src-peer-id opts)
                               src-peer-id]))
                       (remove (comp pos? first))
                       (map second))
        remaining-peers (sequence offer-xf src-peers)]
    (if (empty? remaining-peers)
      (advance state)
      (-> state 
          (initial-sync-backoff)
          (set-context! (assoc context :src-peers remaining-peers))))))

(defn unblock-subscribers [state]
  (sub/unblock! (m/subscriber (get-messenger state)))
  (advance (set-context! state nil)))

(defn trigger-watermarks [state]
  (let [event (get-event state)] 
    (if (windowed-task? event)
      (ws/assign-windows state
                         (assoc (onyx.types/new-state-event :watermark event) 
                                :watermarks (state->watermarks state)))
      state)))

(defn assign-windows [state]
  (let [event (get-event state)
        state-event (if (empty? (mapcat :leaves (:tree (:onyx.core/results event)))) 
                      (onyx.types/new-state-event :task-iteration (get-event state))
                      (onyx.types/new-state-event :new-segment (get-event state)))
        state* (if (watermark-flag? state)
                 (-> state 
                     (trigger-watermarks)
                     (set-watermark-flag! false))
                 state)]
    (-> state*
        (ws/assign-windows state-event)
        (advance))))

(defn build-lifecycle-invoke-fn [event lifecycle-kw]
  (if-let [f (lc/compile-lifecycle-functions event lifecycle-kw)]
    (fn [state]
      (advance (update-event! state f)))))

(defn recover-input [state]
  (let [{:keys [recover-coordinates recovered?] :as context} (get-context state)
        input-pipeline (get-input-pipeline state)]
    (when-not recovered?
      (let [event (get-event state)
            stored (res/recover-input event recover-coordinates)]
        (info (:onyx.core/log-prefix event) "Recover pipeline checkpoint:" stored)
        (p/recover! input-pipeline (t/replica-version state) stored)))
    (if (p/synced? input-pipeline (t/epoch state))
      (-> state
          (set-context! nil)
          (advance))
      ;; ensure we don't try to recover input again before synced
      (set-context! state (assoc context :recovered? true)))))

(defn cleanup-previous-state-store! [state]
  (when-let [state-store (get-state-store state)] 
    (db/drop! state-store)
    (db/close! state-store)))

(defn db-name [event]
  (str (java.util.UUID/randomUUID)))

(defn recover-state
  [state]
  (let [{:keys [onyx.core/task-id onyx.core/peer-opts] :as event} (get-event state)
        _ (cleanup-previous-state-store! state)
        state-serializers (onyx.state.serializers.utils/event->state-serializers event)
        db-name (db-name event)
        state-store (db/create-db peer-opts db-name state-serializers)
        _ (set-state-store! state state-store)
        {:keys [recover-coordinates]} (get-context state)
        recovered-windows (res/recover-windows event state-store recover-coordinates)]
    (-> state
        (set-windows-state! recovered-windows)
        (ws/assign-windows (onyx.types/new-state-event :recovered event))
        (advance))))

(defn recover-output [state]
  (let [{:keys [recover-coordinates recovered?] :as context} (get-context state)
        pipeline (get-output-pipeline state)]
    (when-not recovered?
      (let [event (get-event state)
            ;; output recovery is only supported with onyx/n-peers set
            ;; as we can't currently scale slot recovery up and down
            stored (res/recover-output event recover-coordinates)]
        (info (:onyx.core/log-prefix event) "Recover output pipeline checkpoint:" stored)
        (p/recover! pipeline (t/replica-version state) stored)))
    (if (p/synced? pipeline (t/epoch state))
      (-> state
          (set-context! nil)
          (advance))
      ;; ensure we don't try to recover output again before synced
      (set-context! state (assoc context :recovered? true)))))

(defn event->pub-liveness [event]
  (ms->ns (arg-or-default :onyx.peer/publisher-liveness-timeout-ms 
                          (:onyx.core/peer-opts event))))

(defn poll-recover-check-upstream [state]
  (let [timeout (event->pub-liveness (get-event state))] 
    (reduce evict-peer! 
            state
            (upstream-timed-out-peers (m/subscriber (get-messenger state)) 
                                      timeout))))

(defn poll-recover-input-function [state]
  (let [messenger (get-messenger state)
        subscriber (m/subscriber messenger)]
    (sub/poll! subscriber)
    (poll-recover-check-upstream state)
    (if (and (sub/blocked? subscriber)
             (sub/recovered? subscriber))
      (-> state
          (next-epoch!)
          (set-context! {:recover-coordinates (sub/get-recover subscriber)
                         :recovered? false
                         :barrier-opts {:recover-coordinates (sub/get-recover subscriber)
                                        :checkpoint? false
                                        :completed? false}
                         :src-peers (sub/src-peers subscriber)
                         :publishers (m/publishers messenger)})
          (advance))
      state)))

(defn poll-recover-output [state]
  (let [subscriber (m/subscriber (get-messenger state))]
    (sub/poll! subscriber)
    (poll-recover-check-upstream state)
    (if (and (sub/blocked? subscriber)
             (sub/recovered? subscriber))
      (-> state
          (next-epoch!)
          (set-context! {:recovered? false
                         :recover-coordinates (sub/get-recover subscriber)
                         :src-peers (sub/src-peers subscriber)})
          (advance))
      state)))

(def DEBUG false)

(defn iteration [state n-iters]
  (loop [state (exec state) n n-iters]
    (if (and (advanced? state) (pos? n))
      (recur (exec state) ;; we could unroll exec loop a bit
             (if (new-iteration? state)
               (dec n)
               n))
      state)))

(def task-iterations 1)

(defn run-task-lifecycle!
  "The main task run loop, read batch, write batch, checkpoint, etc."
  [state handle-exception-fn exception-action-fn]
  (try
    (let [{:keys [onyx.core/replica-atom] :as event} (get-event state)]
      (loop [state state
             prev-replica-val (get-replica state)
             replica-val @replica-atom]
        (if (and (= replica-val prev-replica-val)
                 (not (killed? state)))
          (recur (iteration state task-iterations) replica-val @replica-atom)
          (let [next-state (next-replica! state replica-val)]
            (if (killed? next-state)
              (do
                (info (:onyx.core/log-prefix event) "Fell out of task lifecycle loop")
                next-state)
              (recur next-state replica-val replica-val))))))
    (catch Throwable e
      (let [lifecycle (get-lifecycle state)
            action (if (:kill-job? (ex-data e))
                     :kill
                     (exception-action-fn (get-event state) lifecycle e))]
        (handle-exception-fn lifecycle action e))
      state)))

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

(defn build-apply-fn [event]
  (let [f (:onyx.core/fn event)
        a-fn (if (:onyx/batch-fn? (:onyx.core/task-map event))
               transform/apply-fn-batch
               transform/apply-fn-single)]
    (fn [state]
      (transform/apply-fn a-fn f state))))

(defn build-check-publisher-heartbeats [event]
  (let [timeout (event->pub-liveness event)] 
    (fn [state]
      (let [timed-out (upstream-timed-out-peers (m/subscriber (get-messenger state)) timeout)]
        (->> timed-out
             (reduce evict-peer! state) 
             (advance))))))

(defn build-read-batch [{:keys [onyx.core/task-map] :as event}] 
  (if (input-task? event) 
    (let [batch-size (long (:onyx/batch-size task-map))
          batch-timeout (long (ms->ns (arg-or-default :onyx/batch-timeout task-map)))
          apply-watermark (if-let [watermark-fn (:assign-watermark-fn event)]
                            (fn [state]
                              (->> (get-event state)
                                   (:onyx.core/batch)
                                   (run! (fn [seg] (t/process-watermark! state (watermark-fn seg)))))
                              state)
                            (if (satisfies? p/WatermarkedInput (:onyx.core/input-plugin event))
                              (fn [state] 
                                (t/process-watermark! state (p/watermark (get-input-pipeline state)))
                                state)
                              identity))] 
      (fn [state]
        (-> state 
            (read-batch/read-input-batch batch-size batch-timeout)
            (apply-watermark))))
    read-batch/read-function-batch))

(def state-fn-builders
  {:recover [{:lifecycle :lifecycle/poll-recover
              :builder (fn [event] 
                         (if (output-task? event) 
                           poll-recover-output
                           poll-recover-input-function))}
             {:lifecycle :lifecycle/offer-barriers
              :builder (fn [_] offer-barriers)}
             {:lifecycle :lifecycle/offer-barrier-status
              :builder (fn [_] offer-barrier-status)}
             {:lifecycle :lifecycle/recover-input
              :builder (fn [_] recover-input)}
             {:lifecycle :lifecycle/recover-state
              :builder (fn [_] recover-state)}
             {:lifecycle :lifecycle/recover-output
              :builder (fn [_] recover-output)}
             {:lifecycle :lifecycle/unblock-subscribers
              :builder (fn [_] unblock-subscribers)}]
   :start-iteration [{:lifecycle :lifecycle/next-iteration
                      :builder (fn [_] next-iteration)}]
   :barriers [{:lifecycle :lifecycle/input-poll-barriers
               :builder (fn [_] input-poll-barriers)}
              {:lifecycle :lifecycle/check-publisher-heartbeats
               :builder build-check-publisher-heartbeats}
              {:lifecycle :lifecycle/seal-barriers?
               :builder (fn [_] input-function-seal-barriers?)}
              {:lifecycle :lifecycle/seal-barriers?
               :builder (fn [_] output-seal-barriers?)}
              {:lifecycle :lifecycle/checkpoint-input
               :builder (fn [_] checkpoint-input)}
              {:lifecycle :lifecycle/checkpoint-state
               :builder (fn [_] checkpoint-state)}
              {:lifecycle :lifecycle/checkpoint-output
               :builder (fn [_] checkpoint-output)}
              {:lifecycle :lifecycle/offer-barriers
               :builder (fn [_] offer-barriers)}
              {:lifecycle :lifecycle/offer-barrier-status
               :builder (fn [_] offer-barrier-status)}
              {:lifecycle :lifecycle/unblock-subscribers
               :builder (fn [_] unblock-subscribers)}]
   :process-batch [{:lifecycle :lifecycle/before-batch
                    :builder (fn [event] (build-lifecycle-invoke-fn event :lifecycle/before-batch))}
                   {:lifecycle :lifecycle/read-batch
                    :builder build-read-batch}
                   {:lifecycle :lifecycle/check-publisher-heartbeats
                    :builder build-check-publisher-heartbeats}
                   {:lifecycle :lifecycle/after-read-batch
                    :builder (fn [event] (build-lifecycle-invoke-fn event :lifecycle/after-read-batch))}
                   {:lifecycle :lifecycle/apply-fn
                    :builder build-apply-fn}
                   {:lifecycle :lifecycle/after-apply-fn
                    :builder (fn [event] (build-lifecycle-invoke-fn event :lifecycle/after-apply-fn))}
                   {:lifecycle :lifecycle/assign-windows
                    :builder (fn [_] assign-windows)}
                   {:lifecycle :lifecycle/prepare-batch
                    :builder (fn [_] prepare-batch)}
                   {:lifecycle :lifecycle/write-batch
                    :builder (fn [_] write-batch)}
                   {:lifecycle :lifecycle/after-batch
                    :builder (fn [event] (build-lifecycle-invoke-fn event :lifecycle/after-batch))}]
   :heartbeat [{:lifecycle :lifecycle/offer-heartbeats
                :builder (fn [_] offer-heartbeats)}]})

(def lifecycles 
  (let [task-states (get-in onyx.information-model/model [:task-states :model])] 
    (assert (= (count task-states) (count state-fn-builders)))
    (merge-with (fn [infos builders]
                  (mapv (fn [i b]
                          (when-not (= (:lifecycle i) (:lifecycle b))
                            (throw (Exception. (format "State builders and state information model must be in the same order. %s vs %s" i b))))
                          (merge i b))
                        infos
                        builders))
                task-states
                state-fn-builders)))

(defn build-task-fns
  [{:keys [onyx.core/task-map onyx.core/windows onyx.core/triggers] :as event}]
  (let [task-types (cond-> #{(:onyx/type task-map)}
                     (windowed-task? event) (conj :windowed))
        phase-order [:recover :start-iteration :barriers :process-batch :heartbeat]]
    (->> (reduce (fn [accum phase] (into accum (get lifecycles phase))) [] phase-order)
         (filter (fn [lifecycle]
                   ;; see information_model.cljc for :type of task that should 
                   ;; be used with each lifecycle type.
                   (not-empty (clojure.set/intersection task-types (:type lifecycle)))))
         (map (fn [lifecycle] (assoc lifecycle :fn ((:builder lifecycle) event))))
         (vec))))

;; Used in tests to detect when a task stop is called
(defn stop-flag! [])

(defn notify-created-db! [state-store-ch replica-version event state-store]
  (>!! state-store-ch 
       [:created-db 
        replica-version
        (select-keys event [:onyx.core/job-id :onyx.core/task 
                            :onyx.core/slot-id :onyx.core/task-map 
                            :onyx.core/windows :onyx.core/triggers])
        (db/export-reader state-store)]))

(defn setup-checkpoint-watch! [{:keys [onyx.core/log onyx.core/tenancy-id onyx.core/job-id 
                                       onyx.core/task-kill-flag onyx.core/kill-flag] :as event}
                               track-checkpointed]
  (cp/watch-checkpoint-coordinate log tenancy-id job-id 
   (fn [v] 
     (when (= :NodeDataChanged (:event-type v))
       (swap! track-checkpointed 
              merge 
              (select-keys (cp/read-checkpoint-coordinate log tenancy-id job-id) [:epoch :replica-version])))
     (when (and (not @task-kill-flag)
                (not @kill-flag))
       (setup-checkpoint-watch! event track-checkpointed)))))

(deftype TaskStateMachine 
  [monitoring
   subscriber-liveness-timeout-ns
   publisher-liveness-timeout-ns
   initial-sync-backoff-ns
   input-pipeline
   output-pipeline
   state-store-ch
   ^IdleStrategy idle-strategy
   ^int recover-idx
   ^int iteration-idx
   ^int batch-idx
   ^int nstates
   #^"[Lclojure.lang.Keyword;" lifecycle-names
   #^"[Lclojure.lang.IFn;" lifecycle-fns
   ^AtomicInteger idx
   ^:unsynchronized-mutable ^java.lang.Boolean advanced
   ^:unsynchronized-mutable ^java.lang.Boolean sealed
   ^:unsynchronized-mutable ^java.lang.Boolean watermark-flag
   ^:unsynchronized-mutable replica
   ^:unsynchronized-mutable messenger
   messenger-group
   ^:unsynchronized-mutable coordinator
   ^:unsynchronized-mutable state-store
   init-event
   ^:unsynchronized-mutable event
   ^:unsynchronized-mutable windows-state
   ^:unsynchronized-mutable context
   ^:unsynchronized-mutable replica-version
   ^:unsynchronized-mutable epoch
   ^:unsynchronized-mutable watermark
   track-checkpointed
   heartbeat-ns
   ^AtomicLong last-heartbeat
   ^AtomicLong time-init-state
   ^:unsynchronized-mutable evicted]
  t/PTaskStateMachine
  (start [this] 
    (when (or (input-task? event) (output-task? event)) 
      (setup-checkpoint-watch! event track-checkpointed))
    this)
  (stop [this scheduler-event]
    (stop-flag!)
    (when messenger (component/stop messenger))
    (when coordinator (coordinator/stop coordinator scheduler-event))
    (some-> event :onyx.core/storage cp/stop)
    (some-> input-pipeline (p/stop event))
    (some-> output-pipeline (p/stop event))
    this)
  (killed? [this]
    (or @(:onyx.core/task-kill-flag event) @(:onyx.core/kill-flag event)))
  (new-iteration? [this]
    (= (.get idx) iteration-idx))
  (advanced? [this]
    advanced)
  (get-lifecycle [this]
    (aget lifecycle-names (.get idx)))
  (heartbeat! [this]
    (let [curr-time (System/nanoTime)]
      (if (> curr-time (+ (.get last-heartbeat) heartbeat-ns))
        ;; send our status back upstream, and heartbeat
        (let [pubs (m/publishers messenger)
              sub (m/subscriber messenger)
              _ (run! pub/poll-heartbeats! pubs)
              _ (run! pub/offer-heartbeat! pubs)
              opts (assoc (barrier-status-opts this) :event :heartbeat)]
          (run! (fn [peer-id]
                  (sub/offer-barrier-status! sub peer-id opts))
                (sub/src-peers sub))
          (.set last-heartbeat curr-time)
          (set-received-heartbeats! messenger monitoring)
          ;; check if downstream peers are still up
          (let [timed-out (downstream-timed-out-peers pubs subscriber-liveness-timeout-ns)]
            (reduce evict-peer! this timed-out)))
        this)))
  (initial-sync-backoff [this]
    (when (zero? (t/epoch this))
      (LockSupport/parkNanos initial-sync-backoff-ns))
    this)
  (log-state [this]
    (let [task-map (:onyx.core/task-map event)]
      (info "Task state"
            {:type (:onyx/type task-map)
             :name (:onyx/name task-map)
             :slot (:onyx.core/slot-id event)
             :id (:onyx.core/id event) 
             :lifecycle (get-lifecycle this)
             :adv? advanced
             :rv replica-version
             :e epoch
             :n-pubs (count (m/publishers messenger))
             :batch (:onyx.core/batch event)
             :results (:onyx.core/results event)}))
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
  (set-watermark-flag! [this flag]
    (set! watermark-flag flag)
    this)
  (watermark-flag? [this]
    watermark-flag)
  (get-input-pipeline [this]
    input-pipeline)
  (get-output-pipeline [this]
    output-pipeline)
  (seal-checkpoints! [this]
    (let [checkpoints @track-checkpointed] 
      (t/seal-checkpoints! this (:replica-version checkpoints) (:epoch checkpoints))))
  (seal-checkpoints! [this replica-version epoch]
    (let [checkpoints @track-checkpointed] 
      (if (= replica-version (:replica-version checkpoints))
        (let [checkpointed-epoch (:epoch checkpoints)
              epochs (range (inc (:sealed-epoch checkpoints)) 
                            (inc checkpointed-epoch))] 
          (when-not (empty? epochs) 
            (run! (fn [ep]
                    (some-> input-pipeline (p/checkpointed! ep))
                    (some-> output-pipeline (p/checkpointed! ep)))
                  epochs)
            (swap! track-checkpointed 
                   (fn [t]
                     (if (= replica-version (:replica-version t))
                       (assoc t :sealed-epoch checkpointed-epoch)
                       t)))))))
    this)
  (next-replica! [this new-replica]
    (if (= replica new-replica)
      this
      (let [{:keys [onyx.core/id onyx.core/job-id onyx.core/task-id 
                    onyx.core/storage onyx.core/task-kill-flag]} event
            old-version (get-in replica [:allocation-version job-id])
            new-version (get-in new-replica [:allocation-version job-id])]
        (cond (= old-version new-version)
              (-> this
                  (set-coordinator! (coordinator/next-state coordinator replica new-replica))
                  (set-replica! new-replica))

              (let [allocated (common/peer->allocated-job (:allocations new-replica) id)]
                (or (killed? this)
                    (not= task-id (:task allocated))
                    (not= job-id (:job allocated))))
              ;; Manually hit the kill switch early since we've been
              ;; reallocated and we want to escape ASAP
              (do
                (reset! task-kill-flag true)
                this)

              :else
              (let [next-messenger (ms/next-messenger-state! messenger event replica new-replica)]
                (cp/cancel! storage)
                (set! evicted #{})
                (-> this
                    (set-sealed! false)
                    (set-watermark-flag! false)
                    (set-messenger! next-messenger)
                    (set-coordinator! (coordinator/next-state coordinator replica new-replica))
                    (set-replica! new-replica)
                    (reset-event!)
                    (goto-recover!)))))))
  (set-windows-state! [this new-windows-state]
    (set! windows-state new-windows-state)
    this)
  (get-windows-state [this]
    windows-state)
  (set-replica! [this new-replica]
    (set! replica new-replica)
    (let [new-version (get-in new-replica [:allocation-version (:onyx.core/job-id event)])]
      (when-not (= new-version replica-version)
        (reset! track-checkpointed {:replica-version replica-version :epoch 0 :sealed-epoch 0})
        (set-epoch! this initialize-epoch)
        (set! replica-version new-version)))
    this)
  (get-replica [this]
    replica)
  (set-event! [this new-event]
    (set! event new-event)
    this)
  (evict-peer! [this peer-id]
    (let [{:keys [onyx.core/log-prefix onyx.core/log onyx.core/id onyx.core/outbox-ch]} event]
      ;; If we're not up, don't emit a log message. We're probably dead too.
      (when (and (extensions/connected? log)
                 (not (get evicted peer-id)))
        (set! evicted (conj evicted peer-id))
        (let [peer-id (coordinator-peer-id->peer-id peer-id)
              ;; Evict ourselves, as we're not sure whether it's the other peer at fault, or ourselves
              ;; and we may be evicting the peer that would have evicted us.
              evict-self-entry {:fn :leave-cluster
                                :peer-parent id
                                :args {:id id
                                       :group-id (get-in replica [:groups-reverse-index id])}}
              entry {:fn :leave-cluster
                     :peer-parent id
                     :args {:id peer-id
                            :group-id (get-in replica [:groups-reverse-index peer-id])}}]
          (info log-prefix "Peer timed out with no heartbeats. Emitting leave cluster." entry)
          (>!! outbox-ch evict-self-entry)
          (>!! outbox-ch entry))))
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
  (set-state-store! [this new-state-store]
    (set! state-store new-state-store)
    (notify-created-db! state-store-ch replica-version event new-state-store)
    this)
  (get-watermark [this]
     watermark)
  (process-watermark! [this watermark*]
    (set! watermark (max watermark watermark*)))
  (get-state-store [this]
    state-store)
  (set-coordinator! [this next-coordinator]
    (set! coordinator next-coordinator)
    this)
  (goto-recover! [this]
    (.set idx recover-idx)
    (-> this
        (set-context! nil)
        (reset-event!)))
  (goto-next-iteration! [this]
    (.set idx iteration-idx)
    this)
  (goto-next-batch! [this]
    (set! advanced true)
    (.set idx batch-idx)
    this)
  (get-coordinator [this]
    coordinator)
  (exec [this]
    (set! advanced false)
    (let [task-fn (aget lifecycle-fns (.get idx))
          next-state (task-fn this)]
      (if advanced
        (do
         (.set time-init-state (System/nanoTime))
         (.idle idle-strategy 1)
         next-state)
        (do (.idle idle-strategy 0)
            (heartbeat! next-state)))))
  (advance [this]
    (let [new-idx (.incrementAndGet idx)]
      (set! advanced true)
      (if (= new-idx nstates)
        (goto-next-iteration! this)
        this))))

(defn lookup-lifecycle-idx [lifecycles name]
  (->> lifecycles
       (map-indexed (fn [idx v]
                      (if (= name (:lifecycle v))
                        idx)))
       (remove nil?)
       (first)))

(defn wrap-lifecycle-metrics [{:keys [time-init-state] :as monitoring} lifecycle]
  (let [lfn (:fn lifecycle)]
    (if-let [mon-fn (get monitoring (:lifecycle lifecycle))]
      (fn [state]
        (let [next-state (lfn state)
              end (System/nanoTime)
              elapsed (unchecked-subtract end (.get ^AtomicLong time-init-state))]
          (mon-fn next-state elapsed)
          next-state))
      lfn)))

(defn lookup-batch-start-index [lifecycles]
  ;; before-batch may be stripped, thus before or read may be first batch fn
  (int (or (lookup-lifecycle-idx lifecycles :lifecycle/before-batch)
           (lookup-lifecycle-idx lifecycles :lifecycle/read-batch))))

(defn new-state-machine [event peer-config messenger-group coordinator state-store-ch]
  (let [{:keys [onyx.core/input-plugin onyx.core/output-plugin onyx.core/monitoring onyx.core/id 
                onyx.core/log-prefix onyx.core/serialized-task onyx.core/catalog]} event
        {:keys [replica-version] :as base-replica} (onyx.log.replica/starting-replica peer-config)
        {:keys [last-heartbeat time-init-state task-state-index]} monitoring
        lifecycles (filter :fn (build-task-fns event))
        names (into-array clojure.lang.Keyword (mapv :lifecycle lifecycles))
        state-fns (->> lifecycles
                       (mapv #(wrap-lifecycle-metrics monitoring %))
                       (into-array clojure.lang.IFn))
        recover-idx (int 0)
        _ (.set ^AtomicInteger task-state-index recover-idx)
        _ (.set ^AtomicLong time-init-state (System/nanoTime))
        iteration-idx (int (lookup-lifecycle-idx lifecycles :lifecycle/next-iteration))
        batch-idx (lookup-batch-start-index lifecycles)
        heartbeat-ns (ms->ns (arg-or-default :onyx.peer/heartbeat-ms peer-config))
        task->grouping-fn (g/compile-grouping-fn catalog (:egress-tasks serialized-task))
        messenger (m/build-messenger peer-config messenger-group monitoring id task->grouping-fn)
        idle-strategy (BackoffIdleStrategy. 5
                                            5
                                            (arg-or-default :onyx.peer/idle-min-sleep-ns peer-config)
                                            (arg-or-default :onyx.peer/idle-max-sleep-ns peer-config))
        advanced? false
        sealed? false
        watermark-flag false
        watermark 0
        track-checkpointed (atom {})
        evicted #{}]
    (info log-prefix "Starting task state machine:" (mapv vector (range) names))
    (->TaskStateMachine monitoring
                        (ms->ns (arg-or-default :onyx.peer/subscriber-liveness-timeout-ms peer-config))
                        (ms->ns (arg-or-default :onyx.peer/publisher-liveness-timeout-ms peer-config))
                        (ms->ns (arg-or-default :onyx.peer/initial-sync-backoff-ms peer-config))
                        input-plugin output-plugin state-store-ch idle-strategy recover-idx iteration-idx batch-idx
                        (count state-fns) names state-fns task-state-index advanced? sealed? watermark-flag 
                        base-replica messenger messenger-group coordinator nil event event nil nil replica-version 
                        initialize-epoch watermark track-checkpointed heartbeat-ns last-heartbeat
                        time-init-state evicted)))

;; NOTE: currently, if task doesn't start before the liveness timeout, the peer will be killed
;; peer should probably be heartbeating here
(defn backoff-until-task-start!
  [{:keys [onyx.core/kill-flag onyx.core/task-kill-flag onyx.core/opts] :as event} start-fn]
  (while (and (not (or @kill-flag @task-kill-flag))
              (not (start-lifecycle? event start-fn)))
    (Thread/sleep (arg-or-default :onyx.peer/peer-not-ready-back-off opts))))

(defn start-task-lifecycle! [state handle-exception-fn exception-action-fn]
  (thread (run-task-lifecycle! state handle-exception-fn exception-action-fn)))

(defn take-final-state!! [component]
  (<!! (:task-lifecycle-ch component)))

(defn compile-task
  [{:keys [task-information job-id task-id id monitoring log replica-origin
           replica opts outbox-ch group-ch task-kill-flag kill-flag]}]
  (let [{:keys [workflow catalog task flow-conditions resume-point
                windows triggers lifecycles metadata]} task-information
        log-prefix (logger/log-prefix task-information)
        task-map (find-task catalog (:name task))
        task-windows (vec (wc/filter-windows windows (:name task)))
        window-ids (set (map :window/id task-windows))
        task-triggers (filterv (comp window-ids :trigger/window-id) triggers)
        _ (info log-prefix "Compiling lifecycle")]
    (->> {:onyx.core/id id
          :onyx.core/tenancy-id (:onyx/tenancy-id opts)
          :onyx.core/job-id job-id
          :onyx.core/task-id task-id
          :onyx.core/slot-id (get-in replica-origin [:task-slot-ids job-id task-id id])
          :onyx.core/task (:name task)
          :onyx.core/catalog catalog
          :onyx.core/workflow workflow
          :onyx.core/windows task-windows
          :onyx.core/triggers task-triggers
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
    (p/start (instantiate-plugin event) event)))

(defn build-output-pipeline [{:keys [onyx.core/task-map] :as event}]
  (if (= :output (:onyx/type task-map))
    (p/start (instantiate-plugin event) event)
    (p/start (mo/new-messenger-output event) event)))

(defrecord TaskLifeCycle
           [id log messenger-group job-id task-id replica group-ch state-store-ch log-prefix monitoring
            kill-flag outbox-ch completion-ch peer-group opts task-kill-flag
            scheduler-event task-information replica-origin]

  component/Lifecycle
  (start [component]
    (let [peer-error-fn (:peer-error! monitoring)
          handle-exception-fn (fn [lifecycle action e]
                                (peer-error-fn)
                                (handle-exception task-information log 
                                                  e lifecycle action group-ch outbox-ch 
                                                  id job-id))]
      (try
        (let [log-prefix (logger/log-prefix task-information)
              component (assoc component :log-prefix log-prefix)
              event (compile-task component)
              exception-action-fn (lc/compile-lifecycle-handle-exception-functions event)
              start?-fn (lc/compile-start-task-functions event)
              before-task-start-fn (or (lc/compile-lifecycle-functions event :lifecycle/before-task-start) identity)
              after-task-stop-fn (or (lc/compile-lifecycle-functions event :lifecycle/after-task-stop) identity)]
          (try
            (info log-prefix "Warming up task lifecycle" (:onyx.core/serialized-task event))
            (backoff-until-task-start! event start?-fn)
            (try
              (let [{:keys [onyx.core/task-map] :as event} (before-task-start-fn event)
                    task-monitoring (component/start (metrics-monitoring/new-task-monitoring event))
                    component (assoc component :task-monitoring task-monitoring)
                    event (assoc event :onyx.core/monitoring task-monitoring)]
                (try
                 (let [input-pipeline (build-input-pipeline event)
                       output-pipeline (build-output-pipeline event)
                       {:keys [workflow resume-point]} task-information
                       coordinator (new-peer-coordinator workflow resume-point log messenger-group
                                                         task-monitoring opts id job-id group-ch)
                       storage (if (= :zookeeper (arg-or-default :onyx.peer/storage opts))
                                 ;; reuse group zookeeper connection
                                 log
                                 (cp/storage opts task-monitoring))
                       event (assoc event 
                                    :onyx.core/input-plugin input-pipeline
                                    :onyx.core/output-plugin output-pipeline
                                    :onyx.core/monitoring task-monitoring
                                    :onyx.core/storage storage)
                       state (new-state-machine event opts messenger-group coordinator state-store-ch)
                       _ (info log-prefix "Enough peers are active, starting the task")
                       _ (s/validate os/Event event)
                       _ (t/start state)
                       task-lifecycle-ch (start-task-lifecycle! state handle-exception-fn exception-action-fn)]
                   (assoc component
                          :event event
                          :state state
                          :log-prefix log-prefix
                          :task-information task-information
                          :after-task-stop-fn after-task-stop-fn
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
    (some-> component :task-monitoring component/stop)
    (some-> component :kill-flag (reset! true))
    (some-> component :task-kill-flag (reset! true))
    (when-let [event (:event component)]
      (debug (:log-prefix component) "Stopped task. Waiting to fall out of task loop.")
      (when-let [final-state (take-final-state!! component)]
        (info (:log-prefix component) "Task received final state, shutting down task components.")
        (t/stop final-state (:scheduler-event component)))
      (when-let [f (:after-task-stop-fn component)]
        ;; after-task-stop is not guaranteed to complete before the job is sealed
        ;; it should therefore be considered at most once.
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
