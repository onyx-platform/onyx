(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [schema.core :as s]
            [clojure.set :refer [intersection union difference map-invert]]           
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.messenger-state :as ms]
            [onyx.messaging.atom-messenger :as atom-messenger]
            [onyx.log.commands.common :as common]
            ;[onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))


;; Coordinator messenger state just needs the input tasks as the output. Nothing on the input.
;; Peer messenger state needs the coordinator as input, whatever the egress is as output
;; Acking still goes to the input tasks

;; peers need to be able to pick up new "0 epoch" barriers that tell them where to rewind to 
(defn coordinator-publications 
  [{:keys [peer-state allocations peer-sites] :as replica} 
   {:keys [workflow catalog task serialized-task job-id id] :as event}]
  (let [;receivable-peers (job-receivable-peers peer-state allocations job-id)
        receivable-peers (fn [task-id] (get-in allocations [job-id task-id] []))]
    (->> (common/root-tasks workflow task)
         (mapcat (fn [task-id] 
                   (let [peers (receivable-peers task-id)]
                     (map (fn [peer-id]
                            {:src-peer-id id
                             :dst-task-id [job-id task-id]
                             ;; Double check that peer site is correct
                             :site (peer-sites peer-id)})
                          peers))))
         set)))

(defn update-messenger [messenger old-pubs new-pubs]
  (let [remove-pubs (difference old-pubs new-pubs)
        add-pubs (difference new-pubs old-pubs)]
    (as-> messenger m
      (reduce m/remove-publication m remove-pubs)
      (reduce m/add-publication m add-pubs))))

(defprotocol Coordinator
  (start [this])
  (stop [this])
  (emit-barrier [this])
  (next-state [this old-replica new-replica]))

(defrecord PeerCoordinator [messaging-group peer-config peer-id job-id messenger]
  Coordinator
  (start [this] 
    (println "m Group " messaging-group)
    (let [messenger (component/start 
                      (atom-messenger/map->AtomMessenger 
                        {:peer-state {:messaging-group messaging-group}
                         :peer-id peer-id}))])
    (println "Starting coordinator on" peer-id)
    this)
  (stop [this]
    (println "Stopping coordinator on" peer-id)
    this)
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
        ;; Setup new state 
        ;; Setup new messenger 
        ;; and then emit barrier
        (identity)


        ))))

(defn new-peer-coordinator [messenging-group peer-config peer-id job-id]
  (->PeerCoordinator messenging-group peer-config peer-id job-id nil))
