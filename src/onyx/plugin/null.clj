(ns onyx.plugin.null
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.protocols :as p]))

(defrecord NullWriter []
  p/Plugin

  (start [this event] this)

  (stop [this event]
    this)

  p/Checkpointed
  (checkpointed! [this _])
  (checkpoint [this])
  (recover! [this replica-version checkpoint])

  p/BarrierSynchronization
  (completed? [this] true)
  (synced? [this _] true)

  p/Output
  (prepare-batch [this _ _ _]
    true)

  (write-batch [this event replica messenger]
    true))

(defn output [event]
  (map->NullWriter {}))
