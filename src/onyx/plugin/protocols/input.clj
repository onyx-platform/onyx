(ns onyx.plugin.protocols.input)

(defprotocol Input
  (checkpoint [this]
    "Pure function that returns the full checkpoint state,
     that allows the plugin state to be recovered. This checkpoint value
     will be passed in to recover when restarting the task and recovering
     the plugin's state.")

  (segment [this]
    "Returns the segment last read from the input medium.
     Return nil if no segment is available.")

  (next-state [this event]
    "Moves reader to the next state. Returns the reader in the updated state.")

  (recover [this replica-version checkpoint]
    "Recover the state of the plugin from the supplied checkpoint.
     Returns a new reader.")

  (synced? [this epoch])

  (checkpointed! [this epoch]
    "checkpointed! is called whenever an epoch has been fully snapshotted 
     over the entire workflow DAG. At this point it is safe to do any un-recoverable 
     housekeeping for previous epoch's data.")
  
  (completed? [this]
    "Returns true if this all of this input's data has been processed.
     Streaming inputs should always return false."))
