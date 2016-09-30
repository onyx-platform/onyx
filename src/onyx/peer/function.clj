(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.messenger :as m]
            [onyx.log.commands.common :as common]
            [onyx.plugin.onyx-input :as oi]
            [onyx.protocol.task-state :refer :all]
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

(defn read-function-batch [state]
  (let [{:keys [id job-id task-map batch-size] :as event} (get-event state)
        messenger (get-messenger state)
        batch (loop [accum []]
                (let [new-messages (m/poll messenger)]
                  (if (empty? new-messages)
                    accum
                    (let [all (into accum new-messages)] 
                      (if (>= (count all) batch-size)
                        all
                        (recur all))))))]
    (set-event! state (assoc event :batch batch))))

(defn read-input-batch [state]
  (let [{:keys [task-map id job-id task-id] :as event} (get-event state)
        pipeline (get-pipeline state)
        ;_ (println "Read input batch" id (m/all-barriers-seen? (:messenger state)))
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
        (set-pipeline! next-reader)
        (set-event! (assoc event :batch batch)))))
