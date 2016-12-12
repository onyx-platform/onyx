(ns onyx.plugin.protocols.output)

(defprotocol Output
  (prepare-batch [this event])
  (write-batch [this event]))
