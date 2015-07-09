(ns onyx.peer.pipeline-extensions
  "Public API extensions for the virtual peer data pipeline.")

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
