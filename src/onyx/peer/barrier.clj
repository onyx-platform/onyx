(ns ^:no-doc onyx.peer.barrier
  (:require [onyx.log.commands.common :as common]
            [onyx.extensions]
            [taoensso.timbre :as timbre :refer [debug info]]
            [onyx.types :refer [->Barrier]]))

(defn barrier-epoch
  [{:keys [onyx.core/task-map onyx.core/subscription-maps onyx.core/barrier] :as event}]
  (if (= (:onyx/type task-map) :input) 
    (if barrier 
      (:barrier-epoch barrier))
    (let [all-barriers? (not (some (comp nil? deref :barrier) @subscription-maps))]
      (if all-barriers? 
        (first (map (comp :barrier-epoch deref :barrier) @subscription-maps))))))

(defn emit-barrier
  [{:keys [onyx.core/id onyx.core/task-id onyx.core/job-id onyx.core/compiled] :as event}
   messenger replica-val peer-replica-state barrier-epoch]
  (let [downstream-task-ids (vals (:egress-ids (:task @peer-replica-state)))
        ;; FIXME: Shouldn't be sending once per downstream peer. Should be once per downstream task on a single host
        downstream-peers (mapcat #(get-in replica-val [:allocations job-id %]) downstream-task-ids)]
    (doseq [target downstream-peers]
      #_(when-let [site (peer-site peer-replica-state target)]
        (let [b (->Barrier id
                           barrier-epoch 
                           task-id
                           (:task (common/peer->allocated-job (:allocations replica-val) target))
                           nil)]
          (onyx.extensions/send-barrier messenger site b))))))
