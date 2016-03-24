(ns onyx.peer.pipeline-extensions
  "Public API extensions for the virtual peer data pipeline.")

(defprotocol Pipeline
  "Pipeline protocol. All pipelines must implement this protocols i.e. input, output, functions"
  (write-batch [this event]
               "Writes segments to the outgoing data source. Must return a map.")
  (seal-resource [this event]
                 "Closes any resources that remain open during a task being executed.
                 Called once at the end of a task for each virtual peer after the incoming
                 queue has been exhausted. Only called once globally for a single task."

                 "Closes any resources that remain open during a task being executed.
                 Called once at the end of a task for each virtual peer after the incoming
                 queue has been exhausted. Only called once globally for a single task."))
