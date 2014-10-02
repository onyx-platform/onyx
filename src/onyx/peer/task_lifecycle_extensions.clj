(ns onyx.peer.task-lifecycle-extensions
  "Public API extensions for implementors of task lifecycles."
  (:require [onyx.peer.pipeline-extensions :as p-ext]))

(defn name-dispatch [{:keys [onyx.core/task-map]}]
  (:onyx/name task-map))

(defn ident-dispatch [{:keys [onyx.core/task-map]}]
  (:onyx/ident task-map))

(defn type-and-medium-dispatch [{:keys [onyx.core/task-map]}]
  [(p-ext/task-type task-map) (:onyx/medium task-map)])

(defn type-dispatch [{:keys [onyx.core/task-map]}]
  (p-ext/task-type task-map))

(defn merge-api-levels [f event]
  (reduce
   (fn [result g]
     (merge result (f g result)))
   event
   [name-dispatch ident-dispatch type-and-medium-dispatch type-dispatch]))

(defmulti start-lifecycle?
  "Sometimes znode ephemerality is not enough to signal that a task
   can begin executing due to external conditions. Onyx addresses this
   concern by providing this check just before task execution begin.

   Checks if it's acceptable to start task execution.
   Must return a map with key :onyx.core/start-lifecycle?
   and value of type boolean.

   This operation will be retried after a back-off period."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defmulti inject-lifecycle-resources
  "Adds keys to the event map. This function is called once
   at the start of each task each for each virtual peer.
   Keys added may be accessed later in the lifecycle.
   Must return a map."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defmulti inject-temporal-resources
  "Adds keys to the event map. This function is called once
   per pipeline execution per virtual peer. Keys added may
   be accessed later in the lifecycle. Must return a map."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defmulti close-temporal-resources
  "Closes any resources that were opened during a particular lifecycle run.
   Called once for each lifecycle run. Must return a map."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defmulti close-lifecycle-resources
  "Closes any resources that were opened during the execution of a task by a
   virtual peer. Called once at the end of a task for each virtual peer.
   Must return a map."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defn start-lifecycle?* [event]
  (merge-api-levels start-lifecycle? event))

(defn inject-lifecycle-resources* [event]
  (merge-api-levels inject-lifecycle-resources event))

(defn inject-temporal-resources* [event]
  (merge-api-levels inject-temporal-resources event))

(defn close-temporal-resources* [event]
  (merge-api-levels close-temporal-resources event))

(defn close-lifecycle-resources* [event]
  (merge-api-levels close-lifecycle-resources event))

(defmethod start-lifecycle? :default
  [_ event] {:onyx.core/start-lifecycle? true})

(defmethod inject-lifecycle-resources :default
  [_ event] {})

(defmethod inject-temporal-resources :default
  [_ event] {})

(defmethod close-temporal-resources :default
  [_ event] {})

(defmethod close-lifecycle-resources :default
  [_ event] {})

