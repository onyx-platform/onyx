(ns onyx.coordinator.queue.hornetq
  (:require [onyx.coordinator.planning :as planning]
            [onyx.coordinator.extensions :as extensions]))

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [element parent children]
  {:name (:onyx/name element)
   :ingress-queue (:hornetq/queue-name element)
   :egress-queues (planning/egress-queues-to-children children)})

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [element parent children]
  {:name (:onyx/name element)
   :ingress-queue (:hornetq/queue-name element)
   :egress-queues nil})

