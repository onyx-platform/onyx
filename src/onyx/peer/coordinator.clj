(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout promise-chan dropping-buffer chan close! thread]]
            [taoensso.timbre :refer [info error warn trace fatal]]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.messaging.atom-messenger :as atom-messenger]
            [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-state :as ms]
            [onyx.extensions :as extensions]
            [onyx.log.replica]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn required-input-checkpoints [replica job-id]
  (let [recover-tasks (set (get-in replica [:input-tasks job-id]))] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get recover-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [[_ slot-id]]
                          [task-id slot-id :input])
                        peer->slot)))
         set)))

(defn required-state-checkpoints [replica job-id]
  (let [recover-tasks (set (get-in replica [:state-tasks job-id]))] 
    (->> (get-in replica [:task-slot-ids job-id])
         (filter (fn [[task-id _]] (get recover-tasks task-id)))
         (mapcat (fn [[task-id peer->slot]]
                   (map (fn [[_ slot-id]]
                          [task-id slot-id :state])
                        peer->slot)))
         set)))

(defn max-completed-checkpoints [log replica job-id]
  (let [required (clojure.set/union (required-input-checkpoints replica job-id)
                                    (required-state-checkpoints replica job-id))] 
    ;(println "Required " required "from" (extensions/read-checkpoints log job-id))
    (or (->> (extensions/read-checkpoints log job-id)
             (filterv (fn [[k v]]
                        (= required (set (keys v)))))
             (sort-by key)
             last
             first)
        :beginning)))

; (defn recover-checkpoint
;   [{:keys [job-id task-id slot-id] :as event} prev-replica next-replica checkpoint-type]
;   (assert (= slot-id (get-in next-replica [:task-slot-ids job-id task-id (:id event)])))
;   (let [[[rv e] checkpoints] ]
;     (info "Recovering:" rv e)
;     (get checkpoints [task-id slot-id checkpoint-type]))
;   ; (if (some #{job-id} (:jobs prev-replica))
;   ;   (do 
;   ;    (when (not= (required-checkpoints prev-replica job-id)
;   ;                (required-checkpoints next-replica job-id))
;   ;      (throw (ex-info "Slots for input tasks must currently be stable to allow checkpoint resume" {})))
;   ;    (let [[[rv e] checkpoints] (max-completed-checkpoints event next-replica)]
;   ;      (get checkpoints [task-id slot-id]))))
;   )


;; Coordinator TODO
;; Restart peer if coordinator throws exception
;; make sure 

(defn input-publications [replica peer-id job-id]
  (let [allocations (get-in replica [:allocations job-id])
        input-tasks (get-in replica [:input-tasks job-id])]
    (set 
     (mapcat (fn [task]
               (map (fn [id] 
                      {:src-peer-id peer-id
                       :dst-task-id [job-id task]
                       :site (get-in replica [:peer-sites id])})
                    (get allocations task)))
             input-tasks))))

(defn transition-messenger [messenger old-replica new-replica job-id peer-id]
  (let [old-pubs (input-publications old-replica peer-id job-id)
        new-pubs (input-publications new-replica peer-id job-id)
        remove-pubs (clojure.set/difference old-pubs new-pubs)
        add-pubs (clojure.set/difference new-pubs old-pubs)]
    (as-> messenger m
      (reduce m/remove-publication m remove-pubs)
      (reduce m/add-publication m add-pubs))))

(defn emit-reallocation-barrier 
  [{:keys [log job-id peer-id messenger prev-replica] :as state} new-replica]
  (let [new-messenger (-> messenger 
                          (transition-messenger prev-replica new-replica job-id peer-id)
                          (m/set-replica-version (get-in new-replica [:allocation-version job-id])))
        checkpoint-version (max-completed-checkpoints log new-replica job-id)]
    (println "CHECKPOINT VERSION" checkpoint-version)
    ;; FIXME: messenger needs a shutdown ch so it can give up in offers
    ;; TODO: figure out opts in here?
    ;; Should basically be the replica version and epoch to rewind to
    (m/emit-barrier new-messenger {:recover checkpoint-version})))

(defn coordinator-action [action-type {:keys [messenger] :as state} new-replica]
  (case action-type 
    :shutdown (assoc state :messenger (component/stop messenger))
    :periodic-barrier (assoc state :messenger (m/emit-barrier messenger))
    :reallocation (assoc state 
                         :prev-replica new-replica
                         :messenger (emit-reallocation-barrier state new-replica))))

;; FIXME COORDINATOR NOT BEING STOPPED ON TASK CRASH?
(defn start-coordinator! [state]
  (thread
   (try
    (let [;; Generate from peer-config
          barrier-period-ms 500] 
      (loop [state state]
        (let [timeout-ch (timeout barrier-period-ms)
              {:keys [shutdown-ch allocation-ch]} state
              [new-replica ch] (alts!! [shutdown-ch allocation-ch timeout-ch])]
          (cond (= ch shutdown-ch)
                (recur (coordinator-action :shutdown state (:prev-replica state)))

                (= ch timeout-ch)
                (recur (coordinator-action :periodic-barrier state (:prev-replica state)))

                (and (= ch allocation-ch) new-replica)
                (recur (coordinator-action :reallocation state new-replica))))))
    (catch Throwable e
      ;; FIXME: reboot peer group?
      (fatal e "Error in coordinator")))))

(defprotocol Coordinator
  (start [this])
  (stop [this])
  (emit-barrier [this])
  (started? [this])
  (next-state [this old-replica new-replica]))

(defn next-replica [{:keys [allocation-ch] :as coordinator} replica]
  (when (started? coordinator) 
    (>!! allocation-ch replica))
  coordinator)

(defn start-messenger [messenger replica job-id]
  (-> messenger 
      component/start
      ;; Probably bad to have to default to -1, though it will get updated immediately
      (m/set-replica-version (get-in replica [:allocation-version job-id] -1))))

(defrecord PeerCoordinator [log messaging-group peer-config peer-id job-id messenger allocation-ch shutdown-ch coordinator-thread]
  Coordinator
  (start [this] 
    (info "Starting coordinator on:" peer-id)
    (let [initial-replica (onyx.log.replica/starting-replica peer-config)
          ;; Probably do this in coordinator? or maybe not 
          messenger (-> (atom-messenger/map->AtomMessenger
                         {:peer-state {:id [:coordinator peer-id]
                                       :messaging-group messaging-group}})
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
                                    :allocation-ch allocation-ch 
                                    :shutdown-ch shutdown-ch}))))
  (started? [this]
    (true? (:started? this)))
  (stop [this]
    (info "Stopping coordinator on:" peer-id)
    ;; TODO blocking retrieve value from coordinator therad so that we can wait for full shutdown
    (when shutdown-ch
      (close! shutdown-ch))
    (when allocation-ch 
      (close! allocation-ch))
    (info "Coordinator stopped.")
    (assoc this :allocation-ch nil :started? false :shutdown-ch nil :coordinator-thread nil))
  (emit-barrier [this])
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

        (not= (get-in old-replica [:allocation-version job-id])
              (get-in new-replica [:allocation-version job-id]))
        (next-replica new-replica)))))

(defn new-peer-coordinator [log messaging-group peer-config peer-id job-id]
  (map->PeerCoordinator {:log log
                         :messaging-group messaging-group 
                         :peer-config peer-config 
                         :peer-id peer-id 
                         :job-id job-id}))
