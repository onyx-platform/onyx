(ns onyx.coordinator.queue.hornetq
  (:require [onyx.coordinator.extensions :as extensions]))

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [element parent]
  {:name (:onyx/name element)
   :ingress-queue nil
   :egress-queue (:hornetq/queue-name element)})

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [element parent]
  {:name (:onyx/name element)
   :ingress-queue (:hornetq/queue-name element)
   :egress-queue nil})

