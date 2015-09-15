(ns onyx.log.commands.peer-replica-view
  (:require [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info warn]]
            [onyx.peer.operation :as operation]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defrecord PeerReplicaView [backpressure? pick-peer-fns acker-candidates job-id task-id catalog task])

(defn grouping-tasks [catalog]
  (->> catalog
       (filter (fn [task-map]
                 (or (:onyx/group-by-fn task-map)
                     (:onyx/group-by-key task-map))))
       (map :onyx/name)
       (set)))

(defn build-pick-peer-fn [task-id egress-peers grouping?]
  (let [out-peers (egress-peers task-id)] 
    (info "OUTPEERS " out-peers egress-peers task-id)
    (cond (empty? out-peers)
          (fn [_] nil)
          grouping?
          (fn [hash-group] 
            (nth out-peers
                 (mod hash-group
                      (count out-peers))))
          :else
          (fn [_]
            (rand-nth out-peers)))))

(defmethod extensions/peer-replica-view :default 
  [log entry old-replica new-replica diff old-view peer-id opts]
  (let [allocations (:allocations new-replica)
        allocated-job (common/peer->allocated-job allocations peer-id)
        task-id (:task allocated-job)
        job-id (:job allocated-job)]
    (if job-id
      (let [task (if (= task-id (:task-id old-view)) 
                   (:task old-view)
                   (extensions/read-chunk log :task task-id))
            catalog (if (= job-id (:job-id old-view)) 
                      (:catalog old-view)
                      (extensions/read-chunk log :catalog job-id))
            group-tasks (grouping-tasks catalog)
            {:keys [peer-state ackers]} new-replica
            receivable-peers (common/job-receivable-peers peer-state allocations job-id)
            backpressure? (common/backpressure? new-replica job-id)
            pick-peer-fns (->> (:egress-ids task)
                               (map (fn [[task-name task-id]]
                                      (let [grouping? (group-tasks task-name)] 
                                        (vector task-id 
                                                (build-pick-peer-fn task-id receivable-peers grouping?)))))
                               (into {}))
            job-ackers (get ackers job-id)]
        (->PeerReplicaView backpressure? pick-peer-fns job-ackers job-id task-id catalog task))
      (->PeerReplicaView nil nil nil nil nil nil nil))))
