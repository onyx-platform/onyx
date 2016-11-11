(ns onyx.messaging.protocols.endpoint-status)

(defprotocol EndpointStatus
  (start [this])
  (stop [this])
  (ready? [this])
  (set-replica-version! [this new-replica-version])
  (set-endpoint-peers! [this new-peers])
  (liveness [this])
  (poll! [this]))
