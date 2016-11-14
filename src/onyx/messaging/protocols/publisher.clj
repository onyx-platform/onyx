(ns onyx.messaging.protocols.publisher
  (:refer-clojure :exclude [key]))

(defprotocol Publisher
  (pub-info [this])
  (equiv-meta [this pub-info])
  (start [this])
  (stop [this])
  (offer! [this buf])
  (set-replica-version! [this new-replica-version])
  (set-epoch! [this new-epoch])
  (set-endpoint-peers! [this new-peers])
  (ready? [this])
  (alive? [this])
  (heartbeat! [this])
  (offer-ready! [this])
  (offer-heartbeat! [this])
  (poll-heartbeats! [this])
  (key [this]))
