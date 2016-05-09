(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.messenger :as m]
            [onyx.log.commands.common :as common]
            [onyx.plugin.onyx-input :as oi]
            [clj-tuple :as t]
            [onyx.types :as types]
            [onyx.static.uuid :as uuid]
            [onyx.types :refer [map->Barrier map->BarrierAck]]
            [taoensso.timbre :as timbre :refer [debug info]]))

(defn read-function-batch [{:keys [onyx.core/messenger onyx.core/id onyx.core/task-map] :as event}]
  (let [batch-size (:onyx/batch-size task-map)
        messages (m/receive-messages messenger batch-size)]
    (info "Receiving messages " id (:onyx/name (:onyx.core/task-map event)) (m/all-barriers-seen? messenger) messages)
    {:onyx.core/batch messages}))

(defn read-input-batch
  [{:keys [onyx.core/task-map onyx.core/pipeline onyx.core/id onyx.core/task-id] :as event}]
  (let [batch-size (:onyx/batch-size task-map)
        [next-reader 
         batch] (loop [reader pipeline
                       outgoing []]
                  (if (< (count outgoing) batch-size) 
                    (let [next-reader (oi/next-state reader event)
                          segment (oi/segment next-reader)]
                      (if segment 
                        (recur next-reader 
                               (conj outgoing (types/input (uuid/random-uuid) segment)))
                        [next-reader outgoing]))
                    [reader outgoing]))]
    {:onyx.core/pipeline next-reader
     :onyx.core/batch batch}))
