(ns onyx.plugin.protocols.output)

(defprotocol Output
  (synchronized? [this epoch])
  (prepare-batch [this event])
  (write-batch [this event]))
