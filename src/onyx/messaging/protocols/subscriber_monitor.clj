(ns onyx.messaging.protocols.subscriber-monitor)

(defprotocol SubscriberMonitor
  (start [this])
  (stop [this])
  (ready? [this])
  (set-replica-version! [this new-peers new-replica-version])
  (poll! [this]))
