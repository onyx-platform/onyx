(ns onyx.messaging.protocols.handler)

(defprotocol PartialSubscriber 
  (init [this] [this new-ticket])
  (set-replica-version! [this new-replica-version])
  (set-epoch! [this new-epoch])
  (get-recover [this])
  (block! [this])
  (unblock! [this])
  (blocked? [this])
  (completed? [this])
  (ready-session-id [this])
  (get-batch [this]))
