(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [clojure.core.async :refer [>!! poll! promise-chan dropping-buffer chan close! thread]]
            [onyx.static.planning :as planning]
            [taoensso.timbre :refer [debug info error warn trace fatal]]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.messenger-state :as ms]
            [onyx.static.util :refer [ms->ns]]
            [onyx.peer.constants :refer [ALL_PEERS_SLOT]]
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
                                :dst-task-id [job-id task]
                                :dst-peer-ids (set colocated-peers)
                                :short-id (get message-short-ids 
                                               {:src-peer-type :coordinator
                                                :src-peer-id peer-id
                                                :job-id job-id
                                                :dst-task-id task
                                                :msg-slot-id ALL_PEERS_SLOT})
                                :slot-id ALL_PEERS_SLOT
                                :site site})))))
         (set))))

(defn offer-heartbeats
  [{:keys [messenger] :as state}]
  (run! pub/offer-heartbeat! (m/publishers messenger))
  (assoc state :last-heartbeat-time (System/nanoTime)))

(defn offer-barriers
  [{:keys [messenger rem-barriers barrier-opts offering?] :as state}]
  (if offering? 
    (let [_ (run! pub/poll-heartbeats! (m/publishers messenger))
          offer-xf (comp (map (fn [pub]
                                [(m/offer-barrier messenger pub barrier-opts) 
                                 pub]))
                         (remove (comp pos? first))
                         (map second))
          new-remaining (sequence offer-xf rem-barriers)]
      (if (empty? new-remaining)
        (-> state 
            (assoc :checkpoint-version nil)
            (assoc :offering? false)
            (assoc :rem-barriers nil))   
        (do
         ;; sleep for two milliseconds before retrying the offer
         (LockSupport/parkNanos (* 2 1000000))
         (assoc state :rem-barriers new-remaining))))
    state))

(defn write-coordinate [curr-version log tenancy-id job-id coordinate]
  (try (->> curr-version
            (write-checkpoint-coordinate log tenancy-id job-id coordinate)
            (:version))
       (catch KeeperException$BadVersionException bve
         (info "Coordinator failed to write coordinates.
                This is likely due to the coordinator being quarantined, and another coordinator taking over.")
         curr-version)))

(defn complete-job! [state job-id]
  (>!! (:group-ch state) [:send-to-outbox {:fn :complete-job :args {:job-id job-id}}])
  state)

(defn handle-new-replica 
  [{:keys [peer-config resume-point log job-id peer-id messenger curr-replica zk-version completed?] :as state} 
   new-replica]
  (let [{:keys [onyx/tenancy-id]} peer-config
        completed-coordinates (get-in new-replica [:completed-job-coordinates job-id])
        curr-version (get-in curr-replica [:allocation-version job-id])
        new-version (get-in new-replica [:allocation-version job-id])
        reallocated? (not= curr-version new-version)
        complete-job? (and completed-coordinates
                           (not reallocated?) 
                           (not completed?))]
    (cond complete-job? 
          (let [coordinates (merge {:tenancy-id tenancy-id :job-id job-id} 
                                   completed-coordinates)
                next-zk-version (write-coordinate zk-version log tenancy-id job-id coordinates)]
            (-> state
                (complete-job! job-id)
                (assoc :completed? true
                       :zk-version next-zk-version 
                       :curr-replica new-replica)))
          
          reallocated?
          (let [new-messenger (-> messenger 
                                  (m/update-publishers (input-publications new-replica peer-id job-id))
                                  (m/set-replica-version! new-version)
                                  (m/set-epoch! 0))
                coordinates (read-checkpoint-coordinate log tenancy-id job-id)]
            (assoc state 
                   :completed? false
                   :checkpointing? true
                   :offering? true
                   :barrier-opts {:recover-coordinates coordinates}
                   :rem-barriers (m/publishers new-messenger)
                   :curr-replica new-replica
                   :messenger new-messenger))

          :else
          (assoc state :curr-replica new-replica))))

(defn min-downstream-epoch [messenger]
  (->> (m/publishers messenger)
       (map (comp endpoint-status/min-downstream-epoch pub/endpoint-status))
       (apply min)))

(defn periodic-barrier 
  [{:keys [peer-config zk-version workflow-depth log 
           curr-replica job-id messenger offering?] :as state}]
  (if offering?
    ;; No op because hasn't finished emitting last barrier, wait again
    state
    (let [{:keys [onyx/tenancy-id]} peer-config
          job-sealed? (boolean (get-in curr-replica [:completed-job-coordinates job-id]))
          checkpointed-epoch (min-downstream-epoch messenger) 
          write-coordinate? (> checkpointed-epoch 0)
          coordinates {:tenancy-id tenancy-id
                       :job-id job-id
                       :replica-version (m/replica-version messenger) 
                       :epoch checkpointed-epoch}
          ;; get the next version of the zk node, so we can detect when there are other writers
          next-zk-version (if write-coordinate?
                            (write-coordinate zk-version log tenancy-id job-id coordinates)
                            zk-version)
          messenger (m/set-epoch! messenger (inc (m/epoch messenger)))]
      ;; TODO, coordinator can now use the min downstream epoch to checkpoint
      ;; if they also pass up whether they completed, then it can write
      ;; out the final checkpoint and also the complete job message, without
      ;; all the inputs sealing
      (assoc state 
             :offering? true
             :checkpointing? true
             :zk-version next-zk-version
             :rem-barriers (m/publishers messenger)
             :messenger messenger))))

(defn shutdown [{:keys [peer-config log workflow-depth job-id messenger] :as state}]
  (assoc state :messenger (component/stop messenger)))

(defn initialise-state [{:keys [log job-id peer-config] :as state}]
  (let [{:keys [onyx/tenancy-id]} peer-config
        zk-version (assume-checkpoint-coordinate log tenancy-id job-id)] 
    (-> state 
        (assoc :zk-version zk-version)
        (assoc :last-heartbeat-time (System/nanoTime)))))

(defn start-coordinator! 
  [{:keys [allocation-ch shutdown-ch peer-config] :as state}]
  (thread
   (try
    (let [;; FIXME: allow in job data
          ;snapshot-every-n (arg-or-default :onyx.peer/coordinator-snapshot-every-n-barriers peer-config)
          coordinator-max-sleep-ns (ms->ns (arg-or-default :onyx.peer/coordinator-max-sleep-ms peer-config))
          barrier-period-ns (ms->ns (arg-or-default :onyx.peer/coordinator-barrier-period-ms peer-config))
          heartbeat-ns (ms->ns (arg-or-default :onyx.peer/heartbeat-ms peer-config))] 
      (loop [state (initialise-state state)]
        
        (if-let [scheduler-event (poll! shutdown-ch)]
          (shutdown (assoc state :scheduler-event scheduler-event))
          (if-let [new-replica (poll! allocation-ch)]
            ;; Set up reallocation barriers. Will be sent on next recur through :offer-barriers
            (recur (handle-new-replica state new-replica))
            (cond (:offering? state)
                  ;; Continue offering barriers until success
                  (recur (offer-barriers state)) 

                  (> (System/nanoTime) (+ (:last-heartbeat-time state) heartbeat-ns))
                  ;; Immediately offer heartbeats
                  (recur (offer-heartbeats state))

                  (do 
                   (run! pub/poll-heartbeats! (m/publishers (:messenger state)))
                   (and (:checkpointing? state)
                        ;; recovering?
                        (or (zero? (m/epoch (:messenger state)))
                            ;; all checkpoints completed
                            (= (m/epoch (:messenger state)) 
                               (min-downstream-epoch (:messenger state))))))
                  ;; schedule another barrier, after barrier-period-ns not
                  ;; checkpointing as previous one is done this is to ensure
                  ;; forward progress even if checkpoints take much longer
                  ;; than barrier period
                  (recur (assoc state 
                                :checkpointing? false
                                :next-barrier-time (+ (System/nanoTime) barrier-period-ns)))

                  (and (not (:checkpointing? state)) 
                       (> (System/nanoTime) (:next-barrier-time state)))
                  ;; Setup barriers, will be sent on next recur through :offer-barriers
                  (recur (periodic-barrier state))

                  :else
                  (do
                   (LockSupport/parkNanos coordinator-max-sleep-ns)
                   (recur state)))))))
    (catch Throwable e
      (>!! (:group-ch state) [:restart-vpeer (:peer-id state)])
      (fatal e "Error in coordinator")))))

(defprotocol Coordinator
  (start [this])
  (stop [this scheduler-event])
  (started? [this])
  (next-state [this old-replica new-replica]))

(defn next-replica [{:keys [allocation-ch] :as coordinator} replica]
  (when (started? coordinator) 
    (>!! allocation-ch replica))
  coordinator)

(defn start-messenger [messenger replica job-id]
  (-> (component/start messenger) 
      (m/set-replica-version! (get-in replica [:allocation-version job-id] -1))))

(defn stop-coordinator! [{:keys [shutdown-ch allocation-ch]} scheduler-event]
  (when shutdown-ch
    (>!! shutdown-ch scheduler-event)
    (close! shutdown-ch))
  (when allocation-ch 
    (close! allocation-ch)))

(defrecord PeerCoordinator 
  [workflow resume-point log messenger-group peer-config peer-id job-id
   messenger group-ch allocation-ch shutdown-ch coordinator-thread]
  Coordinator
  (start [this] 
    (info "Piggybacking coordinator on peer:" peer-id)
    (let [initial-replica (onyx.log.replica/starting-replica peer-config)
          messenger (-> (m/build-messenger peer-config messenger-group [:coordinator peer-id])
                        (start-messenger initial-replica job-id)) 
          allocation-ch (chan (dropping-buffer 1))
          shutdown-ch (promise-chan)
          workflow-depth (planning/workflow-depth workflow)]
      (assoc this 
             :started? true
             :allocation-ch allocation-ch
             :shutdown-ch shutdown-ch
             :messenger messenger
             :coordinator-thread (start-coordinator! 
                                   {:workflow-depth workflow-depth
                                    :resume-point resume-point
                                    :log log
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
    (info "Stopping coordinator on:" peer-id)
    (stop-coordinator! this scheduler-event)
    (info "Coordinator stopped.")
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
        (next-replica new-replica)))))

(defn new-peer-coordinator 
  [workflow resume-point log messenger-group peer-config peer-id job-id group-ch]
  (map->PeerCoordinator {:workflow workflow
                         :resume-point resume-point
                         :log log
                         :group-ch group-ch
                         :messenger-group messenger-group 
                         :peer-config peer-config 
                         :peer-id peer-id 
                         :job-id job-id}))
