(ns onyx.peer.task-lifecycle-extensions
  "Public API extensions for implementors of plugins.")

(defn name-dispatch [{:keys [onyx.core/task-map]}]
  (:onyx/name task-map))

(defn ident-dispatch [{:keys [onyx.core/task-map]}]
  (:onyx/ident task-map))

(defn type-and-medium-dispatch [{:keys [onyx.core/task-map]}]
  [(:onyx/type task-map) (:onyx/medium task-map)])

(defn type-dispatch [{:keys [onyx.core/task-map]}]
  (:onyx/type task-map))

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

(defmulti read-batch
  "Reads :onyx/batch-size segments off the incoming data source.
   Must return a map with key :onyx.core/batch and value seq representing
   the ingested segments."
  type-and-medium-dispatch)

(defmulti decompress-batch
  "Decompresses the ingested segments. Must return a map
   with key :onyx.core/decompressed and value seq representing the
   decompressed segments."
  type-and-medium-dispatch)

(defmulti strip-sentinel
  "Checks if the sentinel value is present in the ingested batch
   and removes it from downstream propagation when found.
   
   Only required in batch mode on destructive data sources such as queues.
   Must return a map with key :onyx.core/requeue? and boolean true if the sentinel
   should be requeued. Map must also contain :onyx.core/tail-batch? if this is the last
   batch to be ingested.

   It is sometimes possible to see the sentinel value without
   seeing the last batch, due to circumstances such as clustered queues with failures.

   Further, onyx.core/decompressed should be stripped of
   the sentinel value to avoid propagating it to downstream tasks."
  type-and-medium-dispatch)

(defmulti requeue-sentinel
  "Puts the sentinel value back onto the tail of the incoming data source.
   Only required in batch mode on destructive data sources such as queues.
   Must return a map with key :requeued? and value boolean."
  type-and-medium-dispatch)

(defmulti ack-batch
  "Acknowledges the reading of the ingested batch. Must return a map with
   key :onyx.core/acked and value integer representing the number of segments ack'ed."
  type-and-medium-dispatch)

(defmulti apply-fn
  "Applies a function to the decompressed segments. Must return a map with
   key :onyx.core/results and value seq representing the application of the function
   to the segments."
  type-and-medium-dispatch)

(defmulti compress-batch
  "Compresses the segments that result from function application. Must return
   key :onyx.core/compressed and value seq representing the compression of the segments."
  type-and-medium-dispatch)

(defmulti write-batch
  "Writes segments to the outgoing data source. Must return a map."
  type-and-medium-dispatch)

(defmulti close-temporal-resources
  "Closes any resources that were opened during a particular lifecycle run.
   Called once for each lifecycle run. Must return a map."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defmulti close-lifecycle-resources
  "Closes any resources that were opened during the execution of a task by a
   virtual peer. Called once at the end of a task for each virtual peer.
   Must return a map."
  (fn [dispatch-fn event] (dispatch-fn event)))

(defmulti seal-resource
  "Closes any resources that remain open during a task being executed.
   Called once at the end of a task for each virtual peer after the incoming
   queue has been exhausted. Only called once globally for a single task."
  type-and-medium-dispatch)

(defn start-lifecycle?* [event]
  (merge-api-levels start-lifecycle? event))

(defn inject-lifecycle-resources* [event]
  (merge-api-levels inject-lifecycle-resources event))

(defn close-temporal-resources* [event]
  (merge-api-levels close-temporal-resources event))

(defn close-lifecycle-resources* [event]
  (merge-api-levels close-lifecycle-resources event))

(defmethod start-lifecycle? :default
  [_ event] {:onyx.core/start-lifecycle? true})

(defmethod inject-lifecycle-resources :default
  [_ event] {})

(defmethod close-temporal-resources :default
  [_ event] {})

(defmethod close-lifecycle-resources :default
  [_ event] {})

