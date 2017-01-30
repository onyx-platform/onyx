(ns onyx.peer.function
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.protocol.task-state :refer :all]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))


(defrecord NullWriter [event prepared]
  p/Plugin
  (start [this event] this)

  (stop [this event] this)

  o/Output

  (synced? [this epoch]
    true)

  (recover! [this replica-version checkpointed])

  (checkpointed! [this epoch])

  (prepare-batch [this event _]
    true)

  (write-batch
    [this _ _ _]
    true))

(defn function [event]
  (map->NullWriter {:event event}))

