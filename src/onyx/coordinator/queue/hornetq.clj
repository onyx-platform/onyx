(ns onyx.coordinator.queue.hornetq
  (:require [onyx.coordinator.planning :as planning]
            [onyx.coordinator.extensions :as extensions]))

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [element parent children phase]
  {:name (:onyx/name element)
   :ingress-queues (:hornetq/queue-name element)
   :egress-queues (planning/egress-queues-to-children children)
   :phase phase})

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [element parent children phase]
  {:name (:onyx/name element)
   :ingress-queues (:hornetq/queue-name element)
   :egress-queues {}
   :phase phase})

