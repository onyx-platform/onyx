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
            ;[onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

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
    (println "adding " add-pubs " removing " remove-pubs)
    (as-> messenger m
      (reduce m/remove-publication m remove-pubs)
      (reduce m/add-publication m add-pubs))))

(defn emit-reallocation-barrier [messenger old-replica new-replica job-id peer-id]
  (let [new-messenger (-> messenger 
                          (transition-messenger old-replica new-replica job-id peer-id)
                          (m/set-replica-version (get-in new-replica [:allocation-version job-id])))]
    ;; FIXME: messenger needs a shutdown ch so it can give up in offers
    ;; TODO: figure out opts in here?
    ;; Should basically be the replica version and epoch to rewind to
    (m/emit-barrier new-messenger {})))

(defn coordinator-action [action-type job-id peer-id messenger old-replica new-replica]
  (case action-type 
    :shutdown (component/stop messenger)
    :periodic-barrier (m/emit-barrier messenger {})
    :reallocation (emit-reallocation-barrier messenger old-replica new-replica job-id peer-id)))

(defn start-coordinator! [peer-config messenger initial-replica job-id peer-id allocation-ch shutdown-ch]
  (thread
   (try
    (let [;; Generate from peer-config
          barrier-period-ms 50] 
      (loop [old-replica initial-replica 
             messenger messenger]
        (let [timeout-ch (timeout barrier-period-ms)
              [new-replica ch] (alts!! [shutdown-ch allocation-ch timeout-ch])]
          (cond (= ch shutdown-ch)
                (recur old-replica 
                       (coordinator-action :shutdown job-id peer-id messenger old-replica old-replica))

                (= ch timeout-ch)
                (recur old-replica
                       (coordinator-action :periodic-barrier job-id peer-id messenger old-replica old-replica))

                (and (= ch allocation-ch) new-replica)
                (recur new-replica
                       (coordinator-action :reallocation job-id peer-id messenger old-replica new-replica))))))
    (catch Throwable e
      ;; FIXME: reboot peer group?
      (fatal e "Error in coordinator")))))

(defprotocol Coordinator
  (start [this])
  (stop [this])
  (emit-barrier [this])
  (next-state [this old-replica new-replica]))

(defn next-replica [{:keys [allocation-ch started?] :as coordinator} replica]
  (when started? 
    (>!! allocation-ch replica))
  coordinator)

(defn start-messenger [messenger replica job-id]
  (-> messenger 
      component/start
      ;; Probably bad to have to default to -1, though it will get updated immediately
      (m/set-replica-version (get-in replica [:allocation-version job-id] -1))))

(defrecord PeerCoordinator [messaging-group peer-config peer-id job-id messenger allocation-ch shutdown-ch coordinator-thread]
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
             :coordinator-thread (start-coordinator! peer-config messenger initial-replica job-id peer-id allocation-ch shutdown-ch))))
  (stop [this]
    (info "Stopping coordinator on:" peer-id)
    ;; TODO blocking retrieve value from coordinator therad so that we can wait for full shutdown
    (when shutdown-ch
      (close! shutdown-ch)
      ;; TODO, should have a timeout here?
      (<!! shutdown-ch))
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

(defn new-peer-coordinator [messaging-group peer-config peer-id job-id]
  (map->PeerCoordinator {:messaging-group messaging-group 
                         :peer-config peer-config 
                         :peer-id peer-id 
                         :job-id job-id}))
