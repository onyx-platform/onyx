(ns onyx.plugin.null
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.onyx-output :as o]
            [onyx.plugin.onyx-plugin :as p]))

(defrecord NullWriter []
  p/OnyxPlugin

  (start [this] this)

  (stop [this event]
    this)

  o/OnyxOutput

  (prepare-batch [_ state]
    state)

  (write-batch
    [_ state]
    (let [{:keys [results null/last-batch] :as event} (get-event state)
          messenger (get-messenger state)]
      (assert last-batch)
      (reset! last-batch 
              (->> (mapcat :leaves (:tree results))
                   (map :message)
                   (mapv (fn [v] (assoc v :replica-version (m/replica-version messenger))))))
      state)))

(defn output [event]
  (map->NullWriter {}))

(defn inject-in
  [_ lifecycle]
  {:null/last-batch (atom nil)})

(def in-calls
  {:lifecycle/before-task-start inject-in})
