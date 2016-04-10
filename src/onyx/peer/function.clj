(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.peer.barrier :as b]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.plugin.onyx-input :as oi]
            [clj-tuple :as t]
            [onyx.types :as types]
            [onyx.static.uuid :as uuid]
            [onyx.types :refer [map->Barrier map->BarrierAck]]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defn read-function-batch [{:keys [onyx.core/messenger onyx.core/id] :as event}]
  (let [messages (onyx.extensions/receive-messages messenger event)]
    {:onyx.core/batch messages}))

(defn read-input-batch
  [{:keys [onyx.core/task-map onyx.core/pipeline
           onyx.core/id onyx.core/task-id onyx.core/epoch] :as event}]
  (let [batch-size (:onyx/batch-size task-map)
        n-sent @(:onyx.core/n-sent-messages event)
        barrier-gap 5]
    (loop [reader @pipeline
           outgoing []]
      (cond (and (seq outgoing)
                 (zero? (mod (+ n-sent (count outgoing)) barrier-gap)))
            (let [next-epoch (swap! epoch inc)]
              (reset! pipeline (oi/set-epoch reader next-epoch))
              (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
              {:onyx.core/batch outgoing
               :onyx.core/barrier (map->Barrier 
                                    {:src-peer-id id
                                     :barrier-epoch next-epoch
                                     :src-task-id task-id 
                                     :dst-task-id nil 
                                     :msg-id nil})})

            (>= (count outgoing) batch-size)
            (do (reset! pipeline reader)
                (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                {:onyx.core/batch outgoing})

            :else
            (let [next-reader (oi/next-state reader event)
                  segment (oi/segment next-reader)]
              (if segment
                (recur next-reader (conj outgoing (types/input (uuid/random-uuid) segment)))
                (do (reset! pipeline next-reader)
                    (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                    {:onyx.core/batch outgoing})))))))

(defn ack-barrier!
  [{:keys [onyx.core/replica onyx.core/compiled onyx.core/id onyx.core/workflow
           onyx.core/job-id onyx.core/task-map onyx.core/messenger onyx.core/task
           onyx.core/task-id onyx.core/task-state onyx.core/subscription-maps
           onyx.core/barrier]
    :as event}]
  (when (= (:onyx/type task-map) :output)
    (let [replica-val @replica]
      #_(when-let [barrier-epoch (b/barrier-epoch event)]
        (let [root-task-ids (:root-task-ids @task-state)] 
          (doseq [p (mapcat #(get-in @replica [:allocations job-id %]) root-task-ids)]
            (when-let [site (peer-site task-state p)]
              (onyx.extensions/ack-barrier messenger
                                           site
                                           (map->BarrierAck 
                                            {:barrier-epoch barrier-epoch
                                             :job-id job-id
                                             :task-id task-id
                                             :peer-id p
                                             :type :job-completed})))))
        (run! (fn [s] (reset! (:barrier s) nil)) @subscription-maps))))
  event)
