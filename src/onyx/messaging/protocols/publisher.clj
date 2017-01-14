(ns onyx.messaging.protocols.publisher
  (:refer-clojure :exclude [key]))

(defprotocol Publisher
  (info [this])
  (equiv-meta [this pub-info])
  ;; TODO, remove
  (short-id [this])
  (set-short-id! [this new-short-id])
  (start [this])
  (stop [this])
  (offer! [this buf endpoint-epoch])
  (set-replica-version! [this new-replica-version])
  (set-epoch! [this new-epoch])
  (set-endpoint-peers! [this new-peers])
  (endpoint-status [this])
  (timed-out-subscribers [this])
  (ready? [this])
  (alive? [this])
  (heartbeat! [this])
  (offer-ready! [this])
  (offer-heartbeat! [this])
  (poll-heartbeats! [this])
  (key [this]))
