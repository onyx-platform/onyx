(ns onyx.messaging.protocols.publisher
  (:refer-clojure :exclude [key]))

(defprotocol Publisher
  (pub-info [this])
  (equiv-meta [this pub-info])
  (start [this])
  (stop [this])
  (offer! [this buf])
  (heartbeat! [this])
  (key [this])
  (poll-heartbeats! [this]))
