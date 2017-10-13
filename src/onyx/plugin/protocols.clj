(ns onyx.plugin.protocols)

(defprotocol Plugin
  (start [this event]
    "Initialize the plugin, generally assoc'ing any initial state.")

  (stop [this event]
    "Shutdown the input and close any resources that needs to be closed.
     This can also be done using lifecycles."))

(defprotocol Checkpointed
  (recover! [this replica-version checkpoint]
            "Recover the state of the plugin from the supplied checkpoint.
             Returns a new reader.")

  (checkpoint [this]
              "Pure function that returns the full checkpoint state,
               that allows the plugin state to be recovered. This checkpoint value
               will be passed in to recover when restarting the task and recovering
               the plugin's state.")

  (checkpointed! [this epoch]
                 "checkpointed! is called whenever an epoch has been fully snapshotted 
                  over the job's entire directed acyclic graph. At this point it is safe to
                  do any un-recoverable housekeeping for previous epoch's data."))

(defprotocol BarrierSynchronization
  (synced? [this epoch]
           "Returns a bool for whether it is safe to advance to the next epoch.
            All reads from the previous epoch must be completely emitted / discarded before it is safe to return true.")
  (completed? [this]
              "Returns true if this all of this output's data has been emitted, and it is
               safe to complete the job. Streaming inputs should always return false."))

(defprotocol Input
  (poll! [this event timeout-ms]
    "Polls the plugin for a new segment, or returns nil if none are currently available. Plugin should attempt to limit poll time to timeout-ms."))

(defprotocol WatermarkedInput
  (watermark [this] "Returns the latest high water mark timestamp for the input source"))

(defprotocol Output
  (prepare-batch [this event replica messenger]
                 "Called prior to write batch. Primarily used as an initial preparation step before write-batch is called repeatedly. Must return true or false depending on whether it is safe to advance to the next state.")
  (write-batch [this event replica messenger]
               "Write the prepared batch to the storage medium. 
                Must return true or false depending on whether the batch has been fully written. 
                This is so that non-blocking functions can be used."))
