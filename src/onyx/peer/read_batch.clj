(ns ^:no-doc onyx.peer.read-batch
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
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util.concurrent.locks LockSupport]))

(defn read-function-batch [state]
  (let [messenger (get-messenger state)
        batch (m/poll messenger)]
    (if batch
      (-> state 
          (set-event! (assoc (get-event state) :onyx.core/batch (persistent! batch)))
          (advance)) 
      (do
       ;; not ideal to park here, as it's a bit of a special case, however 
       ;; this is the easiest way to achieve a backoff.
       ;; It should be parking for batch-timeout.
       ;; We can't simply block as we will not continue reading barriers.
       (LockSupport/parkNanos (* 50 1000000))
       (advance state)))))

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
    ;(info "Reading batch" "COUNT" (count batch) job-id task-id "peer-id" id #_batch)
    (-> state
        (set-event! (assoc event :onyx.core/batch batch))
        (advance))))
