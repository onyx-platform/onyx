(ns onyx.messaging.protocols.endpoint-status)

(defprotocol EndpointStatus
  (start [this])
  (stop [this])
  (ready? [this])
  (set-replica-version! [this new-replica-version])
  (set-heartbeat-peers! [this new-peers])
  (poll! [this]))
