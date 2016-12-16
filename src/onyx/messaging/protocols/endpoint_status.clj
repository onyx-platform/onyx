(ns onyx.messaging.protocols.endpoint-status)

(defprotocol EndpointStatus
  (start [this])
  (stop [this])
  (ready? [this])
  (info [this])
  (set-replica-version! [this new-replica-version])
  (set-epoch! [this new-epoch])
  (set-endpoint-peers! [this new-peers])
  (min-endpoint-epoch [this])
  (timed-out-subscribers [this])
  (liveness [this])
  (poll! [this]))
