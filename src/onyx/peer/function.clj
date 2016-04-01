(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.peer.operation :as operation]
            [onyx.peer.barrier :refer [ack-barrier?]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.plugin.onyx-input :as oi]
            [clj-tuple :as t]
            [onyx.types :as types]
            [onyx.static.uuid :as uuid]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.types :refer [map->Barrier]]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defn read-function-batch [{:keys [onyx.core/messenger onyx.core/global-watermarks onyx.core/id] :as event}]
  (let [messages (onyx.extensions/receive-messages messenger event)
        barrier? (instance? onyx.types.Barrier (last messages))
        segments (if barrier? (butlast messages) messages)
        barrier (if barrier? (last messages) nil)]
    (when barrier
      (swap! global-watermarks update-in [(:dst-task-id barrier) (:src-peer-id barrier) :barriers (:barrier-epoch barrier)] conj id))
    {:onyx.core/batch segments
     :onyx.core/barrier barrier}))

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
           onyx.core/task-id onyx.core/peer-replica-view onyx.core/global-watermarks
           onyx.core/barrier]
    :as event}]
  (when (= (:onyx/type task-map) :output)
    (let [{:keys [barrier-epoch src-peer-id]} barrier
          replica-val @replica]
      (when (ack-barrier? @replica @global-watermarks (:ingress-ids compiled) event)
        (let [root-task-ids
              (map
               (fn [root-task]
                 (get-in replica-val [:task-name->id job-id root-task]))
               (common/root-tasks workflow task))]
          (doseq [p (mapcat #(get-in @replica [:allocations job-id %]) root-task-ids)]
            (when-let [site (peer-site peer-replica-view p)]
              (onyx.extensions/internal-complete-segment messenger
                                                         {:barrier-epoch barrier-epoch
                                                          :job-id job-id
                                                          :task-id task-id
                                                          :peer-id p
                                                          :type :job-completed}
                                                         site))))))))
