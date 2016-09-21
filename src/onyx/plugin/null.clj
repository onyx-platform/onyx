(ns onyx.plugin.null
  (:require [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.messenger :as m]
            [onyx.plugin.onyx-output :as o]
            [onyx.plugin.onyx-plugin :as p]))

(defrecord NullWriter [event]
  p/OnyxPlugin

  (start [this] this)

  (stop [this event]
    this)

  o/OnyxOutput

  (prepare-batch [_ state]
    state)

  (write-batch
    [_ {:keys [event messenger] :as state}]
    (let [{:keys [results]} event] 
      (update state 
              :event 
              merge 
              {:null/not-written (map (fn [v] 
                                        (assoc v :replica (m/replica-version messenger))) 
                                      (map :message (mapcat :leaves (:tree results))))}))))

(defn output [event]
  (map->NullWriter {:event event}))
