(ns onyx.plugin.null
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.protocols.output :as o]
            [onyx.plugin.protocols.plugin :as p]))

(defrecord NullWriter []
  p/Plugin

  (start [this] this)

  (stop [this event]
    this)

  o/Output

  (prepare-batch [_ state]
    state)

  (write-batch
    [_ state]
    (let [{:keys [results null/last-batch] :as event} (get-event state)
          messenger (get-messenger state)]
      ;; Manually advance for now, since we can't do it that way in messaging batch
      (advance
          (set-event! state (assoc event 
                                   :null/last-batch
                                   (->> (mapcat :leaves (:tree results))
                                        (map :message)
                                        (mapv (fn [v] (assoc v :replica-version (m/replica-version messenger)))))))))))

(defn output [event]
  (map->NullWriter {}))

(defn inject-in
  [_ lifecycle]
  {:null/last-batch []})

(def in-calls
  {:lifecycle/before-task-start inject-in})
