(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [clojure.core.async :refer [>!! <!! poll! promise-chan sliding-buffer chan close! thread]]
            [onyx.static.planning :as planning]
            [taoensso.timbre :refer [debug info error warn trace fatal]]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.messenger-state :as ms]
            [onyx.peer.liveness :refer [downstream-timed-out-peers]]
            [onyx.static.util :refer [ms->ns]]
            [onyx.peer.constants :refer [load-balance-slot-id]]
            [onyx.peer.status :refer [merge-statuses]]
            [onyx.checkpoint :as checkpoint :refer [read-checkpoint-coordinate
                                                    assume-checkpoint-coordinate
                                                    write-checkpoint-coordinate]]
            [onyx.extensions :as extensions]
            [onyx.log.replica]
            [onyx.static.default-vals :refer [arg-or-default]])
  (:import [org.apache.zookeeper KeeperException$BadVersionException]
           [java.util.concurrent.locks LockSupport]))

(defn input-publications [{:keys [peer-sites message-short-ids] :as replica} peer-id job-id]
  (let [allocations (get-in replica [:allocations job-id])
        input-tasks (get-in replica [:input-tasks job-id])
        coordinator-peer-id [:coordinator peer-id]]
    (->> input-tasks
         (mapcat (fn [task]
                   (->> (get allocations task)
                        (group-by (fn [input-peer]
                                    (get peer-sites input-peer)))
                        (map (fn [[site colocated-peers]]
                               {:src-peer-id coordinator-peer-id
                                :job-id job-id
                                :dst-task-id [job-id task]
                                :dst-peer-ids (set colocated-peers)
                                :batch-size 0
                                :short-id (get message-short-ids
                                               {:src-peer-type :coordinator
                                                :src-peer-id peer-id
                                                :job-id job-id
                                                :dst-task-id task
                                                :msg-slot-id load-balance-slot-id})
                                :slot-id load-balance-slot-id
                                :site site})))))
         (set))))

(defn offer-heartbeats
  [{:keys [messenger] :as state}]
  (run! pub/offer-heartbeat! (m/publishers messenger))
  (assoc state :last-heartbeat-time (System/nanoTime)))

(def coordinator-backoff-ms 1)

(defn offer-barriers [{:keys [messenger barrier] :as state}]
  (if (:offering? barrier)
    (let [offer-xf (comp (map (fn [pub]
                                [(m/offer-barrier messenger pub (:opts barrier))
                                 pub]))
                         (remove (comp pos? first))
                         (map second))
          new-remaining (sequence offer-xf (:remaining barrier))]
      (if (empty? new-remaining)
        (update state :barrier merge {:remaining nil
                                      :offering? false})
        (do ;; park and retry
            (LockSupport/parkNanos (ms->ns coordinator-backoff-ms))
            (update state :barrier merge {:remaining new-remaining}))))
    state))

(defn write-coordinate [curr-version log tenancy-id job-id coordinate]
  (try (->> curr-version
            (write-checkpoint-coordinate log tenancy-id job-id coordinate)
            (:version))
       (catch KeeperException$BadVersionException bve
         (throw (Exception. "Coordinator failed to write coordinates.
                             This is likely due to the coordinator being quarantined, and another coordinator taking over.")))))

(defn next-replica
  [{:keys [log job-id peer-id messenger curr-replica tenancy-id] :as state}
   barrier-period-ns
   new-replica]
  (let [curr-version (get-in curr-replica [:allocation-version job-id])
        new-version (get-in new-replica [:allocation-version job-id])
        job-reallocated? (not= curr-version new-version)]
    (if job-reallocated?
      (let [new-messenger (-> messenger
                              (m/update-publishers (input-publications new-replica peer-id job-id))
                              (m/set-replica-version! new-version)
                              (m/set-epoch! 0))
            coordinates (read-checkpoint-coordinate log tenancy-id job-id)]
        (-> state
            (update :job merge {:completed? false
                                :sealing? false})
            (update :barrier merge {:scheduled? false
                                    :offering? true
                                    :remaining (m/publishers new-messenger)
                                    :opts {:checkpoint? false
                                           :watermarks {:coordinator (System/currentTimeMillis)}
                                           :recover-coordinates coordinates}})
            (update :checkpoint merge {:initiated? false
                                       :epoch 0})
            (assoc :messenger new-messenger)
            (assoc :curr-replica new-replica)))
      (assoc state :curr-replica new-replica))))

(defn complete-job
  [{:keys [tenancy-id log job-id messenger checkpoint group-ch] :as state}]
  (info (format "Job %s completed, and final checkpoint has finished. Writing checkpoint coordinates." job-id))
  (let [replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)]
    (let [coordinates {:tenancy-id tenancy-id 
                       :job-id job-id 
                       :replica-version replica-version 
                       :epoch epoch}
          next-write-version (write-coordinate (:write-version checkpoint) log tenancy-id job-id coordinates)]
      (>!! group-ch [:send-to-outbox {:fn :complete-job :args {:job-id job-id}}])
      (-> state
          (update :job merge {:completed? true
                              :sealing? false})
          (update :checkpoint merge {:write-version next-write-version})))))

(defn merged-statuses [messenger]
  (->> (m/publishers messenger)
       (mapcat (comp endpoint-status/statuses pub/endpoint-status))
       (map val)
       (merge-statuses)))

(defn periodic-barrier
  [{:keys [tenancy-id workflow-depth log curr-replica job-id messenger barrier checkpoint] :as state}]
  (if (:offering? barrier)
    ;; No op because hasn't finished emitting last barrier, back-off
    state
    (let [start-checkpoint? (or (:sealing? (:job state))
                                (not (:initiated? checkpoint)))
          messenger (m/set-epoch! messenger (inc (m/epoch messenger)))]
      (cond-> state
        start-checkpoint?
        (update :checkpoint merge {:initiated? true
                                   :epoch (m/epoch messenger)})
        true
        (update :barrier merge {:scheduled? false
                                :offering? true
                                :remaining (m/publishers messenger)
                                :opts {:completed? (:sealing? (:job state))
                                       :watermarks {:coordinator (System/currentTimeMillis)}
                                       :checkpoint? start-checkpoint?}})
        true
        (assoc :messenger messenger)))))

(defn shutdown [{:keys [messenger] :as state}]
  (assoc state :messenger (component/stop messenger)))

(defn initialise-state [{:keys [log job-id tenancy-id] :as state}]
  (let [write-version (assume-checkpoint-coordinate log tenancy-id job-id)]
    (-> state
        (assoc-in [:checkpoint :write-version] write-version)
        (assoc-in [:last-heartbeat-time] (System/nanoTime)))))

(defn checkpoint-complete?
  [{:keys [initiated? epoch] :as checkpoint} status]
  (or (not initiated?)
      (and (not (:checkpointing? status))
           (>= (:min-epoch status) epoch))))

(defn completed-checkpoint [{:keys [checkpoint messenger job-id tenancy-id log] :as state}]
  (let [{:keys [epoch write-version]} checkpoint
        write-coordinate? (> epoch 0)
        coordinates {:tenancy-id tenancy-id
                     :job-id job-id
                     :replica-version (m/replica-version messenger)
                     :epoch epoch}
        ;; get the next version of the zk node, so we can detect when there are other writers
        next-write-version (if write-coordinate?
                             (write-coordinate write-version log tenancy-id job-id coordinates)
                             write-version)]
    (-> state
        (update :barrier    merge {:scheduled? false})
        (update :checkpoint merge {:initiated? false
                                   :write-version next-write-version}))))

(defn schedule-next-barrier [state barrier-period-ns]
  (update state :barrier merge {:scheduled? true
                                :next-barrier-time (+ (System/nanoTime) barrier-period-ns)}))

(defn emit-seal-barrier [state]
  (-> state
      (assoc-in [:job :sealing?] true)
      (periodic-barrier)))

(defn evict-peer! [{:keys [group-ch evicted curr-replica] :as state} peer-id]
  (if-not (get evicted peer-id)
    (let [entry {:fn :leave-cluster
                 :peer-parent [:coordinator (:peer-id state)]
                 :args {:id peer-id
                        :group-id (get-in curr-replica [:groups-reverse-index peer-id])}}]
      (info "Coordinator: timing out peer with no heartbeats. Emitting leave cluster." entry)
      (>!! group-ch [:send-to-outbox entry])
      (update state :evicted conj peer-id))
    state))

(defn check-peer-timeout! [{:keys [messenger subscriber-liveness-timeout] :as state}]
  (let [timed-out (downstream-timed-out-peers (m/publishers messenger) subscriber-liveness-timeout)]
    (reduce evict-peer! state timed-out)))

(defn coordinator-iteration
  [{:keys [messenger checkpoint last-heartbeat-time allocation-ch
           shutdown-ch barrier job job-id curr-replica subscriber-liveness-timeout] :as state}
   coordinator-max-sleep-ns
   barrier-period-ns
   heartbeat-ns]
  (let [_ (run! pub/poll-heartbeats! (m/publishers messenger))
        state (check-peer-timeout! state)
        status (merged-statuses messenger)
        {:keys [sealing? completed?]} job]
    (cond (> (System/nanoTime) (+ last-heartbeat-time heartbeat-ns))
          ;; Immediately offer heartbeats
          (offer-heartbeats state)

          (:offering? barrier)
          ;; Continue offering barriers until success
          (offer-barriers state)

          (and sealing? (checkpoint-complete? checkpoint status))
          (complete-job state)

          (or completed?
              sealing?
              (not= (m/replica-version messenger) (:replica-version status))
              (not= (m/epoch messenger) (:min-epoch status)))
          (do
            (LockSupport/parkNanos coordinator-max-sleep-ns)
            state)

          ;; checkpoint has completed
          (and (:initiated? checkpoint)
               (checkpoint-complete? checkpoint status))
          (completed-checkpoint state)

          (:drained? status)
          (emit-seal-barrier state)

          (and (:scheduled? barrier) (> (System/nanoTime) (:next-barrier-time barrier)))
          (periodic-barrier state)

          (not (:scheduled? barrier))
          (schedule-next-barrier state barrier-period-ns)

          :else
          (do
            (LockSupport/parkNanos coordinator-max-sleep-ns)
            state))))

(defn start-coordinator!
  [{:keys [allocation-ch shutdown-ch peer-config] :as state}]
  (thread
    (try
     (let [coordinator-max-sleep-ns (ms->ns (arg-or-default :onyx.peer/coordinator-max-sleep-ms peer-config))
           barrier-period-ns (ms->ns (arg-or-default :onyx.peer/coordinator-barrier-period-ms peer-config))
           heartbeat-ns (ms->ns (arg-or-default :onyx.peer/heartbeat-ms peer-config))
           result (loop [state (initialise-state state)]
                    (if-let [scheduler-event (poll! shutdown-ch)]
                      (do (shutdown (assoc state :scheduler-event scheduler-event))
                          :shutdown)
                      (if-let [new-replica (poll! allocation-ch)]
                        ;; Set up reallocation barriers. Will be sent on next recur through :offer-barriers
                        (recur (next-replica state barrier-period-ns new-replica))
                        (recur (coordinator-iteration state
                                                      coordinator-max-sleep-ns
                                                      barrier-period-ns
                                                      heartbeat-ns)))))]
       (when-not (= result :shutdown)
         (throw (Exception. "Coordinator not properly shutdown"))))
      (catch Throwable e
        (>!! (:group-ch state) [:restart-vpeer (:peer-id state)])
        (fatal e "Error in coordinator")))))

(defprotocol Coordinator
  (start [this])
  (stop [this scheduler-event])
  (started? [this])
  (next-state [this old-replica new-replica]))

(defn emit-replica [{:keys [allocation-ch] :as coordinator} replica]
  (when (started? coordinator)
    (>!! allocation-ch replica))
  coordinator)

(defn start-messenger [messenger replica job-id]
  (-> (component/start messenger)
      (m/set-replica-version! (get-in replica [:allocation-version job-id] -1))))

(defn stop-coordinator! [{:keys [shutdown-ch allocation-ch peer-id]} scheduler-event]
  (when shutdown-ch
    (info "Stopping coordinator on:" peer-id)
    (>!! shutdown-ch scheduler-event)
    (close! shutdown-ch))
  (when allocation-ch
    (close! allocation-ch)))

(defrecord PeerCoordinator
           [workflow resume-point log messenger-group peer-config peer-id job-id monitoring
            messenger group-ch allocation-ch shutdown-ch coordinator-thread]
  Coordinator
  (start [this]
    (info "Piggybacking coordinator on peer:" peer-id)
    (let [initial-replica (onyx.log.replica/starting-replica peer-config)
          messenger (-> (m/build-messenger peer-config messenger-group monitoring [:coordinator peer-id] {})
                        (start-messenger initial-replica job-id))
          allocation-ch (chan (sliding-buffer 1))
          shutdown-ch (promise-chan)
          workflow-depth (planning/workflow-depth workflow)]
      (assoc this
             :started? true
             :allocation-ch allocation-ch
             :shutdown-ch shutdown-ch
             :messenger messenger
             :coordinator-thread (start-coordinator!
                                  {:tenancy-id (:onyx/tenancy-id peer-config)
                                   :subscriber-liveness-timeout (ms->ns (arg-or-default :onyx.peer/subscriber-liveness-timeout-ms 
                                                                                        peer-config))
                                   :workflow-depth workflow-depth
                                   :resume-point resume-point
                                   :log log
                                   :evicted #{}
                                   :peer-config peer-config
                                   :messenger messenger
                                   :curr-replica initial-replica
                                   :job-id job-id
                                   :peer-id peer-id
                                   :group-ch group-ch
                                   :allocation-ch allocation-ch
                                   :shutdown-ch shutdown-ch}))))
  (started? [this]
    (true? (:started? this)))
  (stop [this scheduler-event]
    (stop-coordinator! this scheduler-event)
    (assoc this :allocation-ch nil :started? false :shutdown-ch nil :coordinator-thread nil))
  (next-state [this old-replica new-replica]
    (let [started? (= (get-in old-replica [:coordinators job-id]) peer-id)
          start? (= (get-in new-replica [:coordinators job-id]) peer-id)]
      (cond-> this
        (and (not started?) start?)
        (start)

        (and started? (not start?))
        (stop :rescheduled)

        :else
        (emit-replica new-replica)))))

(defn new-peer-coordinator
  [workflow resume-point log messenger-group monitoring peer-config peer-id job-id group-ch]
  (map->PeerCoordinator {:workflow workflow
                         :monitoring monitoring
                         :resume-point resume-point
                         :log log
                         :group-ch group-ch
                         :messenger-group messenger-group
                         :peer-config peer-config
                         :peer-id peer-id
                         :job-id job-id}))
