(ns onyx.peer.pipeline-internal-extensions
  (:require [onyx.coordinator.planning :refer [find-task]]))

(defn type-dispatch [event]
  (:onyx/type (find-task (:catalog event) (:task event))))

(defmulti inject-pipeline-resources type-dispatch)

(defmulti close-temporal-resources type-dispatch)

(defmulti close-pipeline-resources type-dispatch)

(defmethod inject-pipeline-resources :default
  [event] {})

(defmethod close-temporal-resources :default
  [event] {})

(defmethod close-pipeline-resources :default
  [event] {})

