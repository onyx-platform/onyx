(ns ^:no-doc onyx.peer.barrier
  (:require [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.log.commands.common :as common]
            [onyx.extensions]
            [onyx.types :refer [->Barrier]]))

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
