(ns onyx.peer.pipeline-extensions
  "Public API extensions for the virtual peer data pipeline."
  (:gen-class :name onyx.peer.pipeline_extensions))

(gen-interface
  :name onyx.peer.IPipelineInput
  :methods [[ack_message [clojure.lang.IPersistentMap java.util.UUID] Object]
            [retry_message [clojure.lang.IPersistentMap java.util.UUID] Object]
            [pending [clojure.lang.IPersistentMap java.util.UUID] Object]
            [drained [clojure.lang.IPersistentMap] boolean]])

(gen-interface
  :name onyx.peer.IPipeline
  :methods [[read_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [write_batch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [seal_resource [clojure.lang.IPersistentMap] Object]])

(defprotocol IPipelineInput 
  ;   "Acknowledges a message at the native level for a batch of message ids.
  ;    Must return a map."
  (ack-message [this event message-id])
  ;   "Releases a message id from the pending message pool and retries it."
  (retry-message [this event message-id])
  ;   "Returns true if this message ID is pending."
  (pending [this event message-id])
  ;   "Returns true if this input resource has been exhausted."
  (drained [this event]))

(defprotocol IPipeline 
  ;   "Reads :onyx/batch-size segments off the incoming data source.
  ;    Must return a map with key :onyx.core/batch and value seq representing
  ;    the ingested segments. The seq must be maps of two keys:

  ;    - :input - A keyword representing the task that the message came from
  ;    - :message - The consumed message"
  (read-batch [this event])
  ;   "Writes segments to the outgoing data source. Must return a map."
  (write-batch [this event])
  ;   "Closes any resources that remain open during a task being executed.
  ;    Called once at the end of a task for each virtual peer after the incoming
  ;    queue has been exhausted. Only called once globally for a single task."

  ;   "Closes any resources that remain open during a task being executed.
  ;    Called once at the end of a task for each virtual peer after the incoming
  ;    queue has been exhausted. Only called once globally for a single task."
  (seal-resource [this event]))
