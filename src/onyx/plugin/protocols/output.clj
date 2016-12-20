(ns onyx.plugin.protocols.output)

(defprotocol Output
  (synced? [this epoch])
  (prepare-batch [this event replica])
  (write-batch [this event messenger replica]))
