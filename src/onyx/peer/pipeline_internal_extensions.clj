(ns onyx.peer.pipeline-internal-extensions
  "Public API extensions for implementors of non-plugin functionality."
  (:require [onyx.coordinator.planning :refer [find-task]]))

(defn type-dispatch [event]
  (:onyx/type (find-task (:catalog event) (:task event))))

(defmulti inject-pipeline-resources
  "Adds keys to the event map. This function is called once
   at the start of each task each for each virtual peer.
   Keys added may be accessed later in the pipeline.
   This function is called *before* the plugin-facing
   inject-pipeline-resources function. Must return a map."
  type-dispatch)

(defmulti close-temporal-resources
  "Closes any resources that were opened during a particular pipeline run.
   Called once for each pipeline run. This function is called *before*
   the plugin-facing close-temporal-resources function.
   Must return a map."
  type-dispatch)

(defmulti close-pipeline-resources
  "Closes any resources that were opened during the execution of a task by a
   virtual peer. Called once at the end of a task for each virtual peer.
   This function is called *before* the plugin-facing close-pipeline-resources
   function. Must return a map."
  type-dispatch)

(defmethod inject-pipeline-resources :default
  [event] {})

(defmethod close-temporal-resources :default
  [event] {})

(defmethod close-pipeline-resources :default
  [event] {})

