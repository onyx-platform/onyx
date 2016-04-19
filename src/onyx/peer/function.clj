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
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defn read-function-batch [{:keys [onyx.core/messenger onyx.core/id] :as event}]
  (let [messages (m/receive-messages messenger)]
    (info "Receiving messages " (m/all-barriers-seen? messenger) messages)
    (Thread/sleep 1000)
    {:onyx.core/batch messages}))

(defn read-input-batch
  [{:keys [onyx.core/task-map onyx.core/pipeline onyx.core/id onyx.core/task-id] :as event}]
  (info "Moving into read batch")
  (Thread/sleep 1000)
  (let [;batch-size (:onyx/batch-size task-map)
        ;barrier-gap 5
        ]
    (loop [reader @pipeline
           outgoing []]
      (let [next-reader (oi/next-state reader event)
            segment (oi/segment next-reader)]
        (if segment
          (recur next-reader (conj outgoing (types/input (uuid/random-uuid) segment)))
          (do (reset! pipeline next-reader)
              (info "Batch is " outgoing)
              {:onyx.core/batch outgoing}))))))

(defn ack-barrier!
  [{:keys [onyx.core/replica onyx.core/compiled onyx.core/id onyx.core/workflow
           onyx.core/job-id onyx.core/task-map onyx.core/messenger onyx.core/task
           onyx.core/task-id onyx.core/subscription-maps
           onyx.core/barrier]
    :as event}]
  (when (= (:onyx/type task-map) :output)
    (let [replica-val @replica]
      #_(when-let [barrier-epoch (b/barrier-epoch event)]
        (let [root-task-ids (:root-task-ids @task-state)] 
          (doseq [p (mapcat #(get-in @replica [:allocations job-id %]) root-task-ids)]
            (when-let [site (peer-site task-state p)]
              (m/ack-barrier messenger
                                           site
                                           (map->BarrierAck 
                                            {:barrier-epoch barrier-epoch
                                             :job-id job-id
                                             :task-id task-id
                                             :peer-id p
                                             :type :job-completed})))))
        (run! (fn [s] (reset! (:barrier s) nil)) @subscription-maps))))
  event)
