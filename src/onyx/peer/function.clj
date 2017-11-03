(ns onyx.peer.function
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.protocol.task-state :refer :all]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.util :refer [kw->fn exception?] :as u]
            [onyx.plugin.protocols :as p]))

(defn throw-exceptions! 
  "Temporary work-around for the fact that flow conditions are not possible
   when using the function plugin as an output task. Long term goal should be
   to determine how to use flow conditions in a clean way with leaf tasks."
  [results]
  (run! (fn [{:keys [leaves]}]
            (run! (fn [segment]
                    (when (exception? segment)
                       (throw segment)))
                  leaves))
          (:tree results))) 

(defrecord NullWriter [event prepared]
  p/Plugin
  (start [this event] this)
  (stop [this event] this)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)
  (completed? [this] 
    true)

  p/Checkpointed
  (recover! [this replica-version checkpointed])
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event _ _]
    true)

  (write-batch [this {:keys [onyx.core/write-batch]} _ _]
    (throw-exceptions! write-batch)
    true))

(defn function [event]
  (map->NullWriter {:event event}))

