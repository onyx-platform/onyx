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
            [taoensso.timbre :as timbre :refer [debug info]]))

(defn read-function-batch [{:keys [state id job-id task-map batch-size] :as event}]
  ;; Returning messenger and messages like this is ugly and only required because of immutable testing
  ;; TODO; try to get around it some how
  (info "reading function batch " job-id (:onyx/name (:task-map event)) id)
  (let [{:keys [messages] :as new-messenger} (m/receive-messages (:messenger state) batch-size)]
    ;(info "Receiving messages" id (:onyx/name (:task-map event)) (m/all-barriers-seen? messenger) messages (= new-messenger messenger))
    ;(info "Done reading function batch" job-id (:onyx/name (:task-map event)) id messages)
    {:state (assoc state :messenger new-messenger)
     :batch messages}))

;; move to another file?
(defn read-input-batch
  [{:keys [task-map state id job-id task-id] :as event}]
  (let [new-messenger (m/receive-messages (:messenger state) 1)] 
    ;; Only once we've received the initial barrier after a reset
    (if (m/ready? new-messenger) 
      (let [batch-size (:onyx/batch-size task-map)
            [next-reader batch] 
            (loop [reader (:pipeline state)
                   outgoing []]
              (if (< (count outgoing) batch-size) 
                (let [next-reader (oi/next-state reader event)
                      segment (oi/segment next-reader)]
                  (if segment 
                    (recur next-reader 
                           (conj outgoing (types/input (random-uuid) segment)))
                    [next-reader outgoing]))
                [reader outgoing]))]
        (info "Reading batch " job-id task-id "peer-id" id batch)
        {:state (assoc state :pipeline next-reader :messenger new-messenger)
         :batch batch})
      {:state (assoc state :messenger new-messenger)})))
