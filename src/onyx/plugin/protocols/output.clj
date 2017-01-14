(ns onyx.plugin.protocols.output)

(defprotocol Output
  (synced? [this epoch])
  (prepare-batch [this event replica])
  (write-batch [this event messenger replica])
  (recover! [this replica-version checkpoint])
  (checkpointed! [this epoch]
    "checkpointed! is called whenever an epoch has been fully snapshotted 
     over the entire workflow DAG. At this point it is safe to do any un-recoverable 
     housekeeping for previous epoch's data."))
