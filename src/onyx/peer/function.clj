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

(defn read-function-batch [{:keys [messenger id task-map batch-size] :as event}]
  ;; Returning messenger and messages like this is ugly and only required because of immutable testing
  ;; TODO; try to get around it some how
  (let [{:keys [messages] :as new-messenger} (m/receive-messages messenger batch-size)]
    (info "Receiving messages " id (:onyx/name (:task-map event)) (m/all-barriers-seen? messenger) messages (= new-messenger messenger))
    (println "reading function batch " messages)
    {:messenger new-messenger
     :batch messages}))

(defn read-input-batch
  [{:keys [task-map pipeline id task-id] :as event}]
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
    (println "reading batch " batch)
    {:pipeline next-reader
     :batch batch}))
