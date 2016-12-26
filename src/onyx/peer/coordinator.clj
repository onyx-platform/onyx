(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout promise-chan 
                                        dropping-buffer chan close! thread]]
            [onyx.static.planning :as planning]
            [taoensso.timbre :refer [debug info error warn trace fatal]]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.messenger-state :as ms]
            [onyx.extensions :as extensions :refer [read-checkpoint-coordinate 
                                                    read-checkpoint-coordinate-version
                                                    write-checkpoint-coordinate]]
            [onyx.log.replica]
            [onyx.static.default-vals :refer [arg-or-default]])
  (:import [org.apache.zookeeper KeeperException$BadVersionException]))

(defn input-publications [{:keys [peer-sites] :as replica} peer-id job-id]
  (let [allocations (get-in replica [:allocations job-id])
        input-tasks (get-in replica [:input-tasks job-id])
        coordinator-peer-id [:coordinator peer-id]]
    (->> input-tasks
         (mapcat (fn [task]
                   (map (fn [id] 
                          (let [site (get peer-sites id)
                                colocated (->> (get allocations task)
                                               (filter (fn [dst-peer-id]
                                                         (= site (get peer-sites dst-peer-id)))) 
                                               set)
                                slot-id -1]
                            (assert (not (empty? colocated)))
                            {:src-peer-id coordinator-peer-id
                             :dst-task-id [job-id task]
                             :dst-peer-ids colocated
                             :short-id (get-in replica [:message-short-ids {:src-peer-type :coordinator
                                                                            :src-peer-id peer-id
                                                                            :job-id job-id
                                                                            :dst-task-id task
                                                                            :msg-slot-id slot-id}])
                             :slot-id slot-id
                             :site site}))
                        (get allocations task))))
         (set))))

(defn offer-heartbeats
  [{:keys [messenger] :as state}]
  (run! pub/offer-heartbeat! (m/publishers messenger))
  (assoc state :last-heartbeat-time (System/currentTimeMillis)))

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
            (assoc :last-barrier-time (System/currentTimeMillis))
            (assoc :checkpoint-version nil)
            (assoc :offering? false)
            (assoc :rem-barriers nil))   
        (assoc state :rem-barriers new-remaining)))
    state))

(defn emit-reallocation-barrier 
  [{:keys [peer-config resume-point log job-id peer-id messenger curr-replica] :as state} 
   new-replica]
  (let [replica-version (get-in new-replica [:allocation-version job-id])
        {:keys [onyx/tenancy-id]} peer-config
        new-messenger (-> messenger 
                          (m/update-publishers (input-publications new-replica peer-id job-id))
                          (m/set-replica-version! replica-version))
        ;; first try to read a checkpoint coordinate from this job
        ;; otherwise try to resume from the resume point
        checkpoint-version (or (read-checkpoint-coordinate log tenancy-id job-id)
                               (and resume-point 
                                    (read-checkpoint-coordinate log 
                                                                (:tenancy-id resume-point)
                                                                (:job-id resume-point))))
        new-messenger (m/next-epoch! new-messenger)]
    (println "CHECKPOINT VERSION" checkpoint-version)
    (println "Read checkpoint coordinator" checkpoint-version)
    (assoc state 
           :last-barrier-time (System/currentTimeMillis)
           :offering? true
           :barrier-opts {:recover-coordinates checkpoint-version}
           :rem-barriers (m/publishers new-messenger)
           :curr-replica new-replica
           :messenger new-messenger)))

(defn write-coordinate [curr-version log tenancy-id job-id coordinate]
  (try 
   (->> curr-version
        (write-checkpoint-coordinate log tenancy-id job-id coordinate)
        (:version))
   (catch KeeperException$BadVersionException bve
     (info "Coordinator failed to write coordinates.
            Ignoring and waiting for coordinator to shutdown." bve)
     curr-version)))

(defn periodic-barrier 
  [{:keys [peer-config coordinate-version workflow-depth log 
           curr-replica job-id messenger offering?] :as state}]
  (if offering?
    ;; No op because hasn't finished emitting last barrier, wait again
    state
    ;; TODO, add explanation of when it's safe to snapshot
    ;; Smarter snapshotting would be beneficial
    ;; This will be required in order to do "at least once" where the barriers are not always synced
    (let [first-snapshot-epoch 2
          job-sealed? (boolean (get-in curr-replica [:completed-job-coordinates job-id]))
          ;; write latest checkpoint coordinates 
          ;; do not allow write to succeed if we are writing to the wrong zookeeper version
          ;; for the coordinate node, as another coordinate may have taken over, or the 
          ;; final coordinates may have already been written by the :complete-job log
          ;; entry
          {:keys [onyx/tenancy-id]} peer-config
          new-version (if (and (not job-sealed?)
                               (>= (m/epoch messenger) 
                                   (+ first-snapshot-epoch workflow-depth)))
                        (let [coord [(m/replica-version messenger) 
                                     (- (m/epoch messenger) workflow-depth)]] 
                          (write-coordinate coordinate-version log tenancy-id job-id coord))
                        coordinate-version)
          messenger (m/next-epoch! messenger)] 
      (assoc state 
             :offering? true
             :coordinate-version coordinate-version
             :barrier-opts {}
             :rem-barriers (m/publishers messenger)
             :messenger messenger))))

(defn shutdown [{:keys [peer-config log workflow-depth 
                        job-id messenger scheduler-event] :as state}]
  (assoc state :messenger (component/stop messenger)))

(defn coordinator-action [{:keys [messenger peer-id job-id] :as state} action-type new-replica]
  (debug "Coordinator action" action-type)
  (assert (if (#{:reallocation-barrier} action-type)
            (some #{job-id} (:jobs new-replica))
            true))
  ; (assert (= peer-id (get-in new-replica [:coordinators job-id])) 
  ;         [peer-id (get-in new-replica [:coordinators job-id])])
  (case action-type 
    :offer-barriers (offer-barriers state)
    :offer-heartbeats (offer-heartbeats state)
    :shutdown (shutdown state)
    :periodic-barrier (periodic-barrier state)
    :reallocation-barrier (emit-reallocation-barrier state new-replica)))

(defn start-coordinator! 
  [{:keys [log job-id peer-config allocation-ch shutdown-ch] :as state}]
  (thread
   (try
    (let [;; FIXME: allow in job data
          barrier-period-ms (arg-or-default :onyx.peer/coordinator-barrier-period-ms peer-config)
          ; TODO
          ;snapshot-every-n (arg-or-default :onyx.peer/coordinator-snapshot-every-n-barriers peer-config)
          coordinator-max-sleep-ms (arg-or-default :onyx.peer/coordinator-max-sleep-ms peer-config)
          heartbeat-ms (arg-or-default :onyx.peer/heartbeat-ms peer-config)
          {:keys [onyx/tenancy-id]} peer-config
          coordinate-version (read-checkpoint-coordinate-version log tenancy-id job-id)] 
      (println "COORDINATE VERSION" coordinate-version)
      (loop [state (-> state 
                       (assoc :coordinate-version coordinate-version)
                       (assoc :last-barrier-time (System/currentTimeMillis))
                       (assoc :last-heartbeat-time (System/currentTimeMillis)))]
        (if-let [scheduler-event (poll! shutdown-ch)]
          (coordinator-action (assoc state :scheduler-event scheduler-event) 
                              :shutdown 
                              (:curr-replica state))
          (let [new-replica (poll! allocation-ch)]
            (cond new-replica
                  ;; Set up reallocation barriers. Will be sent on next recur through :offer-barriers
                  (recur (coordinator-action state :reallocation-barrier new-replica))

                  (< (+ (:last-heartbeat-time state) heartbeat-ms) (System/currentTimeMillis))
                  ;; Immediately offer heartbeats
                  (recur (coordinator-action state :offer-heartbeats (:curr-replica state)))

                  (:offering? state)
                  ;; Continue offering barriers until success
                  (recur (coordinator-action state :offer-barriers (:curr-replica state))) 

                  (< (+ (:last-barrier-time state) barrier-period-ms) 
                     (System/currentTimeMillis))
                  ;; Setup barriers, will be sent on next recur through :offer-barriers
                  (recur (coordinator-action state :periodic-barrier (:curr-replica state)))

                  :else
                  (do
                   (Thread/sleep coordinator-max-sleep-ms)
                   (recur state)))))))
    (catch Throwable e
      (>!! (:group-ch state) [:restart-vpeer (:peer-id state)])
      (fatal e "Error in coordinator")))))

(defprotocol Coordinator
  (start [this])
  (stop [this scheduler-event])
  (started? [this])
  (send-reallocation-barrier? [this old-replica new-replica])
  (next-state [this old-replica new-replica]))

(defn next-replica [{:keys [allocation-ch] :as coordinator} replica]
  (when (started? coordinator) 
    (>!! allocation-ch replica))
  coordinator)

(defn start-messenger [messenger replica job-id]
  (-> messenger 
      component/start
      ;; Probably bad to have to default to -1, though it will get updated immediately
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
  (send-reallocation-barrier? [this old-replica new-replica]
    (and (some #{job-id} (:jobs new-replica))
         (not= (get-in old-replica [:allocation-version job-id])
               (get-in new-replica [:allocation-version job-id]))))
  (next-state [this old-replica new-replica]
    (let [started? (= (get-in old-replica [:coordinators job-id]) 
                      peer-id)
          start? (= (get-in new-replica [:coordinators job-id]) 
                    peer-id)]
      (cond-> this
        (and (not started?) start?)
        (start)

        (and started? (not start?))
        (stop :rescheduled)

        (send-reallocation-barrier? this old-replica new-replica)
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
