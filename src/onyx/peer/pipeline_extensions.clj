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
   the ingested segments. The seq must be maps of two keys:

   - :input - A keyword representing the task that the message came from
   - :message - The consumed message"
  type-and-medium-dispatch)

(defmulti decompress-batch
  "Decompresses the ingested segments. Must return a map
   with key :onyx.core/decompressed and value seq representing the
   decompressed segments."
  type-and-medium-dispatch)

(defmulti apply-fn
  "Applies a function to the decompressed segments. Must return a map with
   key :onyx.core/results and value seq representing the application of the function
   to the segments."
  (fn [event segment]
    (type-and-medium-dispatch event)))

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

(defmulti ack-message
  "Acknowledges a message at the native level for a batch of message ids.
   Must return a map."
  type-and-medium-dispatch)

(defmulti replay-message
  "Releases a message id from storage and tries to replay it. Must return a map."
  type-and-medium-dispatch)

