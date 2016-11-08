(ns onyx.messaging.protocols.channel-status)

(defprotocol ChannelStatus
  (start [this])
  (stop [this])
  (ready? [this])
  (set-replica-version! [this new-replica-version])
  (set-heartbeat-peers! [this new-peers])
  (poll! [this]))
