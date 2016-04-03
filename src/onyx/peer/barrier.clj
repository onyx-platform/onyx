(ns ^:no-doc onyx.peer.barrier
  (:require [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.log.commands.common :as common]
            [onyx.extensions]
            [taoensso.timbre :as timbre :refer [debug info]]
            [onyx.types :refer [->Barrier]]))

(defn all-barriers-seen? [job-allocations global-watermarks-val this-task-id this-peer-id ingress-ids barrier]
  (empty? 
   (filter
    nil? 
    (mapcat
     (fn [task-id] 
       (let [upstream-task-peers (get job-allocations task-id)]
         (map (fn [peer-id]
                (get-in global-watermarks-val [this-task-id peer-id :barriers (:barrier-epoch barrier) this-peer-id]))
              upstream-task-peers)))
     ingress-ids))))

(defn emit-barrier? 
  [replica-val global-watermarks-val ingress-ids
   {:keys [onyx.core/task-map onyx.core/id onyx.core/job-id onyx.core/task-id onyx.core/barrier] :as event}]
  (let [task-type (:onyx/type task-map)]
    (and barrier
         (or (= :input task-type)
             (and (not= :output task-type)
                  (all-barriers-seen? (get-in replica-val [:allocations job-id]) 
                                      global-watermarks-val 
                                      task-id
                                      id
                                      ingress-ids
                                      barrier))))))

(defn ack-barrier? 
  [replica-val global-watermarks-val ingress-ids {:keys [onyx.core/id onyx.core/job-id onyx.core/task-id onyx.core/barrier] :as event}]
  (and barrier
       (all-barriers-seen? (get-in replica-val [:allocations job-id]) 
                           global-watermarks-val 
                           task-id
                           id
                           ingress-ids
                           barrier)))

(defn remove-barriers-from-watermarks [gws replica-val barrier job-id this-peer-id ingress-task-ids]
  (let [allocations (get-in replica-val [:allocations job-id])]
    (if (all-barriers-seen? allocations gws (:dst-task-id barrier) this-peer-id ingress-task-ids barrier)
      (reduce
       (fn [result peer]
         (update-in result [(:dst-task-id barrier) peer :barriers] dissoc (:barrier-epoch barrier)))
       gws
       (common/upstream-peers replica-val ingress-task-ids job-id))
      gws)))

(defn emit-barrier
  [{:keys [onyx.core/id onyx.core/task-id onyx.core/job-id onyx.core/compiled
           onyx.core/barrier onyx.core/global-watermarks] :as event}
   messenger replica-val peer-replica-view]
  (let [downstream-task-ids (vals (:egress-ids (:task @peer-replica-view)))
        downstream-peers (mapcat #(get-in replica-val [:allocations job-id %]) downstream-task-ids)
        {:keys [barrier-epoch src-peer-id]} barrier]
    ;; FIXME: Shouldn't be sending once per downstream peer. Should be once per downstream task on a single host
    (doseq [target downstream-peers]
      (when-let [site (peer-site peer-replica-view target)]
        (let [b (->Barrier id
                           barrier-epoch 
                           task-id
                           (:task (common/peer->allocated-job (:allocations replica-val) target))
                           nil)]
          (onyx.extensions/send-barrier messenger site b))))
    (swap! global-watermarks remove-barriers-from-watermarks replica-val barrier job-id id (:ingress-ids compiled))))
