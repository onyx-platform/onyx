(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout promise-chan dropping-buffer chan close! thread]]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-state :as ms]
            [onyx.extensions :as extensions]
            [onyx.log.replica]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn required-input-checkpoints [replica job-id]
  (let [recover-tasks (get-in replica [:input-tasks job-id])] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get recover-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [[_ slot-id]]
                          [task-id slot-id :input])
                        peer->slot)))
         set)))

(defn required-state-checkpoints [replica job-id]
  (let [recover-tasks (get-in replica [:state-tasks job-id])] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get recover-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [[_ slot-id]]
                          [task-id slot-id :state])
                        peer->slot)))
         set)))

(defn required-checkpoints [replica job-id]
  (clojure.set/union (required-input-checkpoints replica job-id)
                     (required-state-checkpoints replica job-id)))

(defn max-completed-checkpoints [log replica job-id]
  (let [required (required-checkpoints replica job-id)] 
    ;(println "Required " required "from" (extensions/read-checkpoints log job-id))
    (or (extensions/latest-full-checkpoint log job-id required)
        :beginning)))

(defn input-publications [replica peer-id job-id]
  (let [allocations (get-in replica [:allocations job-id])
        input-tasks (get-in replica [:input-tasks job-id])]
    (set 
     (mapcat (fn [task]
               (map (fn [id] 
                      {:src-peer-id [:coordinator peer-id]
                       :dst-task-id [job-id task]
                       :slot-id -1
                       :site (get-in replica [:peer-sites id])})
                    (get allocations task)))
             input-tasks))))

(defn transition-messenger [messenger old-replica new-replica job-id peer-id]
  (let [old-pubs (input-publications old-replica peer-id job-id)
        new-pubs (input-publications new-replica peer-id job-id)
        remove-pubs (clojure.set/difference old-pubs new-pubs)
        add-pubs (clojure.set/difference new-pubs old-pubs)]
    (as-> messenger m
      (reduce m/add-publication m add-pubs)
      (reduce m/remove-publication m remove-pubs))))

(defn offer-barriers 
  [{:keys [messenger rem-barriers barrier-opts offering?] :as state}]
  (assert messenger)
  (if offering? 
    (loop [pubs rem-barriers]
      (if-not (empty? pubs)
        (let [pub (first pubs)
              ret (m/offer-barrier messenger pub barrier-opts)]
          (if (pos? ret)
            (recur (rest pubs))
            (assoc state :rem-barriers pubs)))
        (-> state 
            (update :messenger m/unblock-subscriptions!)
            (assoc :checkpoint-version nil)
            (assoc :offering? false)
            (assoc :rem-barriers nil))))
    state))

(defn emit-reallocation-barrier 
  [{:keys [log job-id peer-id messenger prev-replica] :as state} new-replica]
  (let [new-messenger (-> messenger 
                          (transition-messenger prev-replica new-replica job-id peer-id)
                          (m/set-replica-version! (get-in new-replica [:allocation-version job-id])))
        checkpoint-version (max-completed-checkpoints log new-replica job-id)
        _ (println "REALLOCATING, TRYING TO RECOVER" checkpoint-version)
        new-messenger (m/next-epoch! new-messenger)]
    (assoc state 
           :offering? true
           :barrier-opts {:recover checkpoint-version}
           :rem-barriers (m/publications new-messenger)
           :prev-replica new-replica
           :messenger new-messenger)))

(defn periodic-barrier [{:keys [prev-replica job-id messenger offering?] :as state}]
  (if offering?
    ;; No op because hasn't finished emitting last barrier, wait again
    state
    (let [messenger (m/next-epoch! messenger)] 
      (assoc state 
             :offering? true
             :barrier-opts {}
             :rem-barriers (m/publications messenger)
             :messenger messenger))))

(defn coordinator-action [action-type {:keys [messenger peer-id job-id] :as state} new-replica]
  (assert 
   (if (#{:reallocation-barrier} action-type)
     (some #{job-id} (:jobs new-replica))
     true))
  (assert (= peer-id (get-in new-replica [:coordinators job-id])) [peer-id (get-in new-replica [:coordinators job-id])])
  (case action-type 
    :offer-barriers (offer-barriers state)
    :shutdown (assoc state :messenger (component/stop messenger))
    :periodic-barrier (periodic-barrier state)
    :reallocation-barrier (emit-reallocation-barrier state new-replica)))

(defn start-coordinator! [state]
  (thread
   (try
    (let [;; Generate from peer-config
          ;; FIXME: put in job data
          barrier-period-ms 500] 
      (loop [state state]
        (let [timeout-ch (timeout barrier-period-ms)
              {:keys [shutdown-ch allocation-ch]} state
              [v ch] (if (:offering? state)
                       (alts!! [shutdown-ch allocation-ch] :default true)
                       (alts!! [shutdown-ch allocation-ch timeout-ch]))]
          (assert (:messenger state))

          (cond (= ch shutdown-ch)
                (recur (coordinator-action :shutdown state (:prev-replica state)))

                (= ch timeout-ch)
                (recur (coordinator-action :periodic-barrier state (:prev-replica state)))

                (true? v)
                (recur (coordinator-action :offer-barriers state (:prev-replica state)))

                (and (= ch allocation-ch) v)
                (recur (coordinator-action :reallocation-barrier state v))))))
    (catch Throwable e
      (>!! (:group-ch state) [:restart-vpeer (:peer-id state)])
      (fatal e "Error in coordinator")))))

(defprotocol Coordinator
  (start [this])
  (stop [this])
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

(defn stop-coordinator! [{:keys [shutdown-ch allocation-ch]}]
  (when shutdown-ch
    (close! shutdown-ch))
  (when allocation-ch 
    (close! allocation-ch)))

(defrecord PeerCoordinator [log messenger-group peer-config peer-id job-id messenger
                            group-ch allocation-ch shutdown-ch coordinator-thread]
  Coordinator
  (start [this] 
    (info "Starting coordinator on:" peer-id)
    (let [initial-replica (onyx.log.replica/starting-replica peer-config)
          ;; Probably do this in coordinator? or maybe not 
          messenger (-> (m/build-messenger peer-config messenger-group [:coordinator peer-id])
                        (start-messenger initial-replica job-id)) 
          allocation-ch (chan (dropping-buffer 1))
          shutdown-ch (promise-chan)]
      (assoc this 
             :started? true
             :allocation-ch allocation-ch
             :shutdown-ch shutdown-ch
             :messenger messenger
             :coordinator-thread (start-coordinator! 
                                   {:log log
                                    :peer-config peer-config 
                                    :messenger messenger 
                                    :prev-replica initial-replica 
                                    :job-id job-id
                                    :peer-id peer-id 
                                    :group-ch group-ch
                                    :allocation-ch allocation-ch 
                                    :shutdown-ch shutdown-ch}))))
  (started? [this]
    (true? (:started? this)))
  (stop [this]
    (info "Stopping coordinator on:" peer-id)
    (stop-coordinator! this)
    ;; TODO blocking retrieve value from coordinator thread so that we can wait for full shutdown
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
        (stop)

        (send-reallocation-barrier? this old-replica new-replica)
        (next-replica new-replica)))))

(defn new-peer-coordinator [log messenger-group peer-config peer-id job-id group-ch]
  (map->PeerCoordinator {:log log
                         :group-ch group-ch
                         :messenger-group messenger-group 
                         :peer-config peer-config 
                         :peer-id peer-id 
                         :job-id job-id}))
