(ns onyx.peer.pipeline-extensions
  "Public API extensions for the virtual peer data pipeline.")

(defn task-type [task-map]
  (if (or (:onyx/group-by-key task-map) (:onyx/group-by-fn task-map))
    :aggregator
    (:onyx/type task-map)))

(defn type-and-medium-dispatch [{:keys [onyx.core/task-map]}]
  [(task-type task-map) (:onyx/medium task-map)])

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

(defmulti seal-resource
  "Closes any resources that remain open during a task being executed.
   Called once at the end of a task for each virtual peer after the incoming
   queue has been exhausted. Only called once globally for a single task."
  type-and-medium-dispatch)

