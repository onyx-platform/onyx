(ns onyx.peer.pipeline-extensions
  "Public API extensions for the virtual peer data pipeline."
  (:gen-class :name onyx.peer.pipeline_extensions))

(gen-interface
  :name onyx.peer.IPipelineInput
  :methods [[ackMessage [clojure.lang.IPersistentMap java.util.UUID] Object]
            [retryMessage [clojure.lang.IPersistentMap java.util.UUID] Object]
            [isPending [clojure.lang.IPersistentMap java.util.UUID] Object]
            [isDrained [clojure.lang.IPersistentMap] boolean]])

(gen-interface
  :name onyx.peer.IPipeline
  :methods [[readBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [writeBatch [clojure.lang.IPersistentMap] clojure.lang.IPersistentMap]
            [sealResource [clojure.lang.IPersistentMap] Object]])

(defprotocol PipelineInput 
  "Input pipeline protocol. All input pipelines must implement this protocol."
  (ack-message [this event message-id]
               "Acknowledges a message at the native level for a batch of message ids.
               Must return a map.")
  (retry-message [this event message-id]
                 "Releases a message id from the pending message pool and retries it.")
  (pending? [this event message-id]
            "Returns true if this message ID is pending.")
  (drained? [this event]
            "Returns true if this input resource has been exhausted."))

(extend-protocol PipelineInput
  onyx.peer.IPipelineInput
  (ack-message [this event message-id]
    (.ackMessage this event message-id))
  (retry-message [this event message-id]
    (.retryMessage this event message-id))
  (pending? [this event message-id]
    (.isPending this event message-id))
  (drained? [this event]
    (.isDrained this event)))

(defprotocol Pipeline 
  "Pipeline protocol. All pipelines must implement this protocols i.e. input, output, functions"
  (read-batch [this event]
              "Reads :onyx/batch-size segments off the incoming data source.
              Must return a map with key :onyx.core/batch and value seq representing
              the ingested segments. The seq must be maps of two keys:

              - :input - A keyword representing the task that the message came from
              - :message - The consumed message")
  (write-batch [this event]
               "Writes segments to the outgoing data source. Must return a map.")
  (seal-resource [this event]
                 "Closes any resources that remain open during a task being executed.
                 Called once at the end of a task for each virtual peer after the incoming
                 queue has been exhausted. Only called once globally for a single task."

                 "Closes any resources that remain open during a task being executed.
                 Called once at the end of a task for each virtual peer after the incoming
                 queue has been exhausted. Only called once globally for a single task."))

(extend-protocol Pipeline
  onyx.peer.IPipeline
  (read-batch [this event]
    (.readBatch this event))
  (write-batch [this event]
    (.writeBatch this event))
  (seal-resource [this event]
    (.sealResource this event)))

