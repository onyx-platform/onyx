(ns onyx.messaging.protocols.status-publisher)

(defprotocol PStatusPublisher
  (start [this])
  (stop [this])
  (offer-heartbeat! [this replica-version epoch]) 
  (offer-ready-reply! [this replica-version epoch session-id]) 
  (offer-barrier-aligned! [this replica-version epoch session-id]))
