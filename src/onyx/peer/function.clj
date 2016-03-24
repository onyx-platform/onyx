(ns ^:no-doc onyx.peer.function
  (:require [clojure.core.async :refer [chan >! go alts!! close! timeout]]
            [onyx.static.planning :refer [find-task]]
            [onyx.messaging.acking-daemon :as acker]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.operation :as operation]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.plugin.simple-input :as si]
            [clj-tuple :as t]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.types :refer [->Barrier]]
            [onyx.plugin.simple-input :refer [SimpleInput]]
            [taoensso.timbre :as timbre :refer [debug info]])
  (:import [java.util UUID]))

(defn read-function-batch [event]
  (let [messages (onyx.extensions/receive-messages (:onyx.core/messenger event) event)
        barrier? (instance? onyx.types.Barrier (last messages))
        segments (if barrier? (butlast messages) messages)
        barrier (if barrier? (last messages) nil)]
    (when barrier
      (swap! (:onyx.core/barrier-state event) assoc (:from-peer-id barrier) true))
    {:onyx.core/batch segments
     :onyx.core/barrier barrier}))

(defn read-input-batch [event]
  (let [batch-size (:onyx/batch-size (:onyx.core/task-map event))
        n-sent @(:onyx.core/n-sent-messages event)
        barrier-gap 5]
    (loop [reader @(:onyx.core/pipeline event)
           outgoing []]
      (cond (and (pos? (+ n-sent (count outgoing)))
                 (zero? (mod (+ n-sent (count outgoing)) barrier-gap)))
            (do (reset! (:onyx.core/pipeline event) reader)
                (swap! (:onyx.core/n-sent-messages event) + (inc (count outgoing)))
                {:onyx.core/batch outgoing
                 :onyx.core/barrier (->Barrier nil (:onyx.core/irnd event)
                                               (+ n-sent (count outgoing))
                                               (:onyx.core/task-id event)
                                               nil (:onyx.core/id event))})

            (>= (count outgoing) batch-size)
            (do (reset! (:onyx.core/pipeline event) reader)
                (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                {:onyx.core/batch outgoing})

            :else
            (let [next-reader (si/next-state reader event)
                  segment (si/segment next-reader)]
              (if segment
                (recur next-reader (conj outgoing segment))
                (do (reset! (:onyx.core/pipeline event) reader)
                    (swap! (:onyx.core/n-sent-messages event) + (count outgoing))
                    {:onyx.core/batch outgoing})))))))

;; Stop reading messages when its time to inject a barrier
;; Only read up to batch-size messages
;; Inject a barrier every n messages

(defn emit-barrier? [replica compiled barrier-state job-id]
  (let [upstream-peers (reduce
                        (fn [result task-id]
                          (into result (get-in replica [:allocations job-id task-id])))
                        []
                        (:ingress-ids compiled))]
    (= (into #{} (keys barrier-state))
       (into #{} upstream-peers))))

(defn emit-barrier [event messenger replica-val peer-replica-view]
  (let [downstream-task-ids (vals (:egress-ids (:task @peer-replica-view)))
        downstream-peers (mapcat #(get-in replica-val [:allocations (:onyx.core/job-id event) %]) downstream-task-ids)]
    (doseq [target downstream-peers]
      (when-let [site (peer-site peer-replica-view target)]
        (let [b (->Barrier target
                           (:onyx.core/id event)
                           (:barrier-id (:onyx.core/barrier event))
                           (:onyx.core/task-id event)
                           (:task (common/peer->allocated-job (:allocations replica-val) target))
                           (:origin-peer (:onyx.core/barrier event)))]
          (onyx.extensions/send-barrier messenger site b))))
    (reset! (:onyx.core/barrier-state event) {})))

(defn ack-barrier!
  [{:keys [onyx.core/replica onyx.core/compiled
           onyx.core/barrier-state onyx.core/job-id
           onyx.core/peer-replica-view onyx.core/barrier]
    :as event}]
  (when (= (:onyx.core/task event) :out)
    (when (emit-barrier? @replica compiled @barrier-state job-id)
      (when-let [site (peer-site peer-replica-view (:origin-peer barrier))]
        (onyx.extensions/internal-complete-segment (:onyx.core/messenger event)
                                                   {:barrier-id (:barrier-id barrier)
                                                    :job-id (:onyx.core/job-id event)
                                                    :task-id (:onyx.core/task-id event)
                                                    :type :job-completed}
                                                    site)))))

(defn write-batch
  ([{:keys [onyx.core/results onyx.core/messenger onyx.core/state
            onyx.core/replica onyx.core/peer-replica-view
            onyx.core/serialized-task onyx.core/barrier] :as event}]
   (write-batch event replica peer-replica-view state messenger (:egress-ids serialized-task)))
  ([event replica peer-replica-view state messenger egress-tasks]
   (let [segments (:segments (:onyx.core/results event))]
     (when-not (empty? segments)
       (let [replica-val @replica
             pick-peer-fns (:pick-peer-fns @peer-replica-view)
             grouped (group-by #(t/vector (:route %) (:hash-group %)) segments)]
         (run! (fn [[[route hash-group] segs]]
                 (let [segs (map #(assoc % :dst-task (get egress-tasks route)) segs)]
                   (when-let [pick-peer-fn (get pick-peer-fns (get egress-tasks route))]
                     (when-let [target (pick-peer-fn hash-group)]
                       (when-let [site (peer-site peer-replica-view target)]
                         (onyx.extensions/send-messages messenger site segs))))))
               grouped)
         (when (emit-barrier? replica-val (:onyx.core/compiled event) @(:onyx.core/barrier-state event) (:onyx.core/job-id event))
           (emit-barrier event messenger replica-val peer-replica-view)))))
   {}))

(defrecord Function [replica peer-replica-view state messenger egress-tasks]

  p-ext/Pipeline

  (write-batch
    [_ event]
    (write-batch event replica peer-replica-view state messenger egress-tasks))

  (seal-resource [_ _]
    nil))

(defn function [{:keys [onyx.core/replica
                        onyx.core/peer-replica-view
                        onyx.core/state
                        onyx.core/messenger
                        onyx.core/serialized-task] :as pipeline-data}]
  (->Function replica
              peer-replica-view
              state
              messenger
              (:egress-ids serialized-task)))
