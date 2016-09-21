(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.messenger :as m]
            [onyx.log.commands.common :as common]
            [onyx.plugin.onyx-input :as oi]
            [clj-tuple :as t]
            [onyx.types :as types]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.types :refer [map->Barrier map->BarrierAck]]
            [onyx.plugin.onyx-plugin :as op]
            [taoensso.timbre :as timbre :refer [debug info]]))

(defrecord FunctionPlugin []
  op/OnyxPlugin
  (start [this] this)
  (stop [this event] this))

(defn read-function-batch [{:keys [event messenger] :as state}]
  (let [{:keys [id job-id task-map batch-size]} event
        batch (loop [accum []]
                (let [new-messages (m/poll messenger)]
                  (if (empty? new-messages)
                    accum
                    (let [all (into accum new-messages)] 
                      (if (>= (count all) batch-size)
                        all
                        (recur all))))))]
    (assoc state :event (assoc event :batch batch))))

(defn read-input-batch [{:keys [event pipeline] :as state}]
  (let [{:keys [task-map id job-id task-id]} event
        batch-size (:onyx/batch-size task-map)
        [next-reader batch] 
        (loop [reader pipeline
               outgoing []]
          (assert pipeline)
          (if (< (count outgoing) batch-size) 
            (let [next-reader (oi/next-state reader event)
                  segment (oi/segment next-reader)]
              (if segment 
                (recur next-reader 
                       (conj outgoing (types/input (random-uuid) segment)))
                [next-reader outgoing]))
            [reader outgoing]))]
    (info "Reading batch" job-id task-id "peer-id" id batch)
    (-> state
        (assoc :pipeline next-reader)
        (assoc :event (assoc event :batch batch)))))
