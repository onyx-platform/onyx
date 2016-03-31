(ns ^:no-doc onyx.peer.barrier
  (:require [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.log.commands.common :as common]
            [onyx.extensions]
            [onyx.types :refer [->Barrier]]))

(defn upstream-peers [replica compiled job-id task-id]
  (reduce
   (fn [result task-id]
     (into result (get-in replica [:allocations job-id task-id])))
   []
   (:ingress-ids compiled)))

(defn emit-barrier? [replica compiled barrier-state job-id task-id barrier-id event]
  (let [up (upstream-peers replica compiled job-id task-id)]
    (or (and (= :input (:onyx/type (:onyx.core/task-map event)))
             (:onyx.core/barrier event))
        (and (not= :output (:onyx/type (:onyx.core/task-map event)))
             (:onyx.core/barrier event)
             (= (into #{} (get-in barrier-state [barrier-id :peers]))
                (into #{} up))))))

(defn ack-barrier? [replica compiled barrier-state job-id task-id barrier-id event]
  (let [up (upstream-peers replica compiled job-id task-id)]
    (and (= :output (:onyx/type (:onyx.core/task-map event)))
         (:onyx.core/barrier event)
         (= (into #{} (get-in barrier-state [barrier-id :peers]))
            (into #{} up)))))

(defn emit-barrier [event messenger replica-val peer-replica-view barrier-state]
  (let [downstream-task-ids (vals (:egress-ids (:task @peer-replica-view)))
        downstream-peers (mapcat #(get-in replica-val [:allocations (:onyx.core/job-id event) %]) downstream-task-ids)
        barrier-val @barrier-state]
    (doseq [target downstream-peers]
      (when-let [site (peer-site peer-replica-view target)]
        (let [b (->Barrier target
                           (:onyx.core/id event)
                           (:barrier-id (:onyx.core/barrier event))
                           (:onyx.core/task-id event)
                           (:task (common/peer->allocated-job (:allocations replica-val) target))
                           (or (get-in barrier-val [(:barrier-id (:onyx.core/barrier event)) :origins])
                               (:origin-peers (:onyx.core/barrier event)))
                           nil)]
          (onyx.extensions/send-barrier messenger site b))))
    (swap! (:onyx.core/global-watermarks event)
           update-in
           [(:from-peer-id (:onyx.core/barrier event)) :barriers (:msg-id (:onyx.core/barrier event))]
           (fnil conj #{}) (:onyx.core/id event))
    (swap! (:onyx.core/barrier-state event) dissoc (:barrier-id (:onyx.core/barrier event)))))
