(ns onyx.peer.function
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.protocol.task-state :refer :all]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.util :refer [kw->fn exception?] :as u]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

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

  o/Output

  (synced? [this epoch]
    true)

  (recover! [this replica-version checkpointed])

  (checkpoint [this])

  (checkpointed! [this epoch])

  (prepare-batch [this event _ _]
    true)

  (write-batch [this {:keys [onyx.core/results]} _ _]
    (throw-exceptions! results)
    true))

(defn function [event]
  (map->NullWriter {:event event}))

