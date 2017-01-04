(ns onyx.plugin.null
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

(defrecord NullWriter []
  p/Plugin

  (start [this event] this)

  (stop [this event]
    this)

  o/Output

  (synced? [this _]
    true)

  (checkpointed! [this _])

  (prepare-batch [this _ _]
    true)

  (write-batch
    [this {:keys [onyx.core/results null/last-batch] :as event} replica messenger]
    (reset! last-batch 
            (->> (mapcat :leaves (:tree results))
                 (mapv (fn [v] (assoc v :replica-version (m/replica-version messenger))))))
    true))

(defn output [event]
  (map->NullWriter {}))

(defn inject-in
  [_ lifecycle]
  {:null/last-batch (atom nil)})

(def in-calls
  {:lifecycle/before-task-start inject-in})
