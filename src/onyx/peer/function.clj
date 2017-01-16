(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.log.commands.common :as common]
            [onyx.plugin.protocols.input :as oi]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t]
            [onyx.types :as types]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.types]
            [taoensso.timbre :as timbre :refer [debug info]]))

(defn read-function-batch [state]
  (let [messenger (get-messenger state)
        batch (m/poll messenger)]
    (if batch
      (-> state 
          (set-event! (assoc (get-event state) :onyx.core/batch (persistent! batch)))
          (advance)) 
      (advance state))))

(defn read-input-batch [state]
  (let [{:keys [onyx.core/task-map onyx.core/id 
                onyx.core/job-id onyx.core/task-id] :as event} (get-event state)
        pipeline (get-input-pipeline state)
        batch-size (:onyx/batch-size task-map)
        batch (persistent! 
               (loop [outgoing (transient [])]
                 (if (< (count outgoing) batch-size) 
                   (if-let [segment (oi/poll! pipeline event)] 
                     (recur (conj! outgoing segment))
                     outgoing)
                   outgoing)))]
    (info "Reading batch" "COUNT" (count batch) job-id task-id "peer-id" id #_batch)
    (-> state
        (set-event! (assoc event :onyx.core/batch batch))
        (advance))))
