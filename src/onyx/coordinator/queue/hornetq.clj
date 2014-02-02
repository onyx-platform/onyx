(ns onyx.coordinator.queue.hornetq
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.planning :as planning]
            [onyx.coordinator.extensions :as extensions])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defrecord HornetQ [addr]
  component/Lifecycle

  (start [component]
    (prn "Starting HornetQ")

    (let [tc (TransportConfiguration. (.getName NettyConnectorFactory))
          locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
          session-factory (.createSessionFactory locator)]
      (assoc component :session-factory session-factory)))

  (stop [component]
    (prn "Stopping HornetQ")
    component))

(defn hornetq [addr]
  (map->HornetQ {:addr addr}))

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [element parent children phase]
  {:name (:onyx/name element)
   :ingress-queues (:hornetq/queue-name element)
   :egress-queues (planning/egress-queues-to-children children)
   :phase phase
   :consumption (:onyx/consumption element)})

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [element parent children phase]
  {:name (:onyx/name element)
   :ingress-queues (get (:egress-queues parent) (:onyx/name element))
   :egress-queues {:self (:hornetq/queue-name element)}
   :phase phase
   :consumption (:onyx/consumption element)})

(defmethod extensions/create-queue :hornetq
  [queue task]
  )

(defmethod extensions/cap-queue :hornetq
  [queue task] true)

