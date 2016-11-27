(ns onyx.messaging.protocols.status-publisher)

(defprotocol PStatusPublisher
  (start [this])
  (stop [this])
  (info [this])
  (set-heartbeat! [this])
  (set-session-id! [this session-id])
  (offer-heartbeat! [this replica-version epoch]) 
  (offer-ready-reply! [this replica-version epoch]) 
  (offer-barrier-aligned! [this replica-version epoch]))
