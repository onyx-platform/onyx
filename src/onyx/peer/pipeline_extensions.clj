(ns onyx.peer.pipeline-extensions
  (:require [onyx.coordinator.planning :refer [find-task]]))

(defn type-and-medium-dispatch [event]
  (let [t (find-task (:catalog event) (:task event))]
    [(:onyx/type t) (:onyx/medium t)]))

(defn ident-dispatch [event]
  (:onyx/ident (find-task (:catalog event) (:task event))))

(defmulti inject-pipeline-resources ident-dispatch)

(defmulti read-batch type-and-medium-dispatch)

(defmulti decompress-batch type-and-medium-dispatch)

(defmulti requeue-sentinel type-and-medium-dispatch)

(defmulti ack-batch type-and-medium-dispatch)

(defmulti apply-fn type-and-medium-dispatch)

(defmulti compress-batch type-and-medium-dispatch)

(defmulti write-batch type-and-medium-dispatch)

(defmulti close-temporal-resources ident-dispatch)

(defmulti close-pipeline-resources ident-dispatch)

(defmulti seal-resource type-and-medium-dispatch)

(defmethod inject-pipeline-resources :default
  [event] {})

(defmethod close-temporal-resources :default
  [event] {})

(defmethod close-pipeline-resources :default
  [event] {})

