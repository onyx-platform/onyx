(ns onyx.plugin.protocols.output)

(defprotocol Output
  (synced? [this epoch]
           "Returns a bool for whether it is safe to advance to the next epoch.
            All writes from the previous epoch (e.g. async writes) must be completely written before it is safe to return true.")
  (prepare-batch [this event messenger replica]
                 "Called prior to write batch. Primarily used as an initial preparation step before write-batch is called repeatedly. Must return true or false depending on whether it is safe to advance to the next state.")
  (write-batch [this event messenger replica]
               "Write the prepared batch to the storage medium. 
                Must return true or false depending on whether the batch has been fully written. 
                This is so that non-blocking functions can be used.")
  (recover! [this replica-version checkpoint]
            "Recover the plugin state using the supplied checkpoint.")
  (checkpoint [this]
              "Pure function that returns the full checkpoint state,
               that allows the plugin state to be recovered. This checkpoint value
               will be passed in to recover when restarting the task and recovering
               the plugin's state.")
  (checkpointed! [this epoch]
    "checkpointed! is called whenever an epoch has been fully snapshotted 
     over the entire workflow DAG. At this point it is safe to do any un-recoverable 
     housekeeping for previous epoch's data."))
