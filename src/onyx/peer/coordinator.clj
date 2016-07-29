(ns onyx.peer.coordinator
  (:require [com.stuartsierra.component :as component]
            [onyx.schema :as os]
            [schema.core :as s]
            [onyx.monitoring.measurements :refer [emit-latency emit-latency-value]]
            [onyx.messaging.messenger :as m]
            ;[onyx.messaging.messenger-replica :as ms]
            ;[onyx.extensions :as extensions]
            [onyx.types :refer [->Results ->MonitorEvent map->Event dec-count! inc-count!]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defprotocol Coordinator
  (start [this])
  (stop [this])
  (emit-barrier [this])
  (next-state [this old-replica new-replica]))

(defrecord PeerCoordinator [peer-config peer-id job-id messenger]
  Coordinator
  (start [this] 
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

(defn new-peer-coordinator [peer-config peer-id job-id]
  (->PeerCoordinator peer-config peer-id job-id nil))
