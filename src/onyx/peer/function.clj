(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.messaging.acking-daemon :as acker]
            [onyx.peer.operation :as operation]
            [onyx.peer.barrier :refer [emit-barrier?]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.plugin.onyx-input :as oi]
            [clj-tuple :as t]
            [onyx.types :as types]
            [onyx.static.uuid :as uuid]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.types :refer [->Barrier]]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defn read-function-batch [event]
  (let [messages (onyx.extensions/receive-messages (:onyx.core/messenger event) event)
        barrier? (instance? onyx.types.Barrier (last messages))
        segments (if barrier? (butlast messages) messages)
        barrier (if barrier? (last messages) nil)]
    (when barrier
      (swap!
       (:onyx.core/barrier-state event)
       (fn [barrier-state]
         (-> barrier-state
             (update-in [(:barrier-id barrier) :peers] (fnil conj #{}) (:from-peer-id barrier))
             (update-in [(:barrier-id barrier) :origins] (fnil into #{}) (:origin-peers barrier))))))
    {:onyx.core/batch segments
     :onyx.core/barrier barrier}))

(defn read-input-batch [event]
  (let [batch-size (:onyx/batch-size (:onyx.core/task-map event))
        n-sent @(:onyx.core/n-sent-messages event)
        barrier-gap 5]
    (loop [reader @(:onyx.core/pipeline event)
           outgoing []]
      (cond (and (seq outgoing)
                 (zero? (mod (+ n-sent (count outgoing)) barrier-gap)))
            (do (reset! (:onyx.core/pipeline event) reader)
                (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                {:onyx.core/batch outgoing
                 :onyx.core/barrier (->Barrier nil (:onyx.core/id event)
                                               (+ n-sent (count outgoing))
                                               (:onyx.core/task-id event)
                                               nil [(:onyx.core/id event)])})

            (>= (count outgoing) batch-size)
            (do (reset! (:onyx.core/pipeline event) reader)
                (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                {:onyx.core/batch outgoing})

            :else
            (let [next-reader (oi/next-state reader event)
                  segment (oi/segment next-reader)]
              (if segment
                (recur next-reader (conj outgoing (types/input (uuid/random-uuid) segment)))
                (do (reset! (:onyx.core/pipeline event) next-reader)
                    (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                    {:onyx.core/batch outgoing})))))))

(defn ack-barrier!
  [{:keys [onyx.core/replica onyx.core/compiled
           onyx.core/barrier-state onyx.core/job-id
           onyx.core/task-id onyx.core/peer-replica-view
           onyx.core/barrier]
    :as event}]
  (when (= (:onyx/type (:onyx.core/task-map event)) :output)
    (when (emit-barrier? @replica compiled @barrier-state job-id task-id (:barrier-id barrier))
      (doseq [p (:origin-peers barrier)]
        (when-let [site (peer-site peer-replica-view p)]
          (onyx.extensions/internal-complete-segment (:onyx.core/messenger event)
                                                     {:barrier-id (:barrier-id barrier)
                                                      :job-id (:onyx.core/job-id event)
                                                      :task-id (:onyx.core/task-id event)
                                                      :peer-id p
                                                      :type :job-completed}
                                                     site))))))
