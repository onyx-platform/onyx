(ns onyx.plugin.protocols.input)

(defprotocol Input
  (checkpoint [this]
    "Pure function that returns the full checkpoint state,
     that allows the plugin state to be recovered. This checkpoint value
     will be passed in to recover when restarting the task and recovering
     the plugin's state.")

  (poll! [this event]
    "Polls the plugin for a new segment, or returns nil if none are currently available.")

  (recover! [this replica-version checkpoint]
    "Recover the state of the plugin from the supplied checkpoint.
     Returns a new reader.")

  (synced? [this epoch]
           "Returns a bool for whether it is safe to advance to the next epoch.
            All reads from the previous epoch must be completely emitted / discarded before it is safe to return true.")

  (checkpointed! [this epoch]
    "checkpointed! is called whenever an epoch has been fully snapshotted 
     over the job's entire directed acyclic graph. At this point it is safe to
     do any un-recoverable housekeeping for previous epoch's data.")
  
  (completed? [this]
    "Returns true if this all of this input's data has been read, and it is
     safe to complete the job. Streaming inputs should always return false."))
