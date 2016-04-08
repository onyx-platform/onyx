(ns onyx.log.commands.peer-replica-view
  (:require [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.messaging.aeron :as aeron]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn peer-site [peer-replica-view peer-id]
  (get (:peer-sites @peer-replica-view) peer-id))

(defrecord PeerReplicaView
  [egress-pubs barrier-pubs root-task-ids job-id task-id workflow catalog task])

(defn build-pick-peer-fn
  [replica job-id my-peer-id task-id task-map egress-peers slot-id->peer-id peer-config]
  (let [out-peers (egress-peers task-id)
        choose-f (cts/choose-downstream-peers replica job-id peer-config my-peer-id out-peers)]
    (cond (empty? out-peers)
          (fn [_] nil)

          (and (planning/grouping-task? task-map) (#{:continue :kill} (:onyx/flux-policy task-map)))
          (fn [hash-group] 
            (nth out-peers
                 (mod hash-group
                      (count out-peers))))

          (and (planning/grouping-task? task-map) (= :recover (:onyx/flux-policy task-map)))
          (let [n-peers (or (:onyx/n-peers task-map)
                            (:onyx/max-peers task-map))] 
            (fn [hash-group] 
              (slot-id->peer-id (mod hash-group n-peers))))

          (planning/grouping-task? task-map) 
          (throw (ex-info "Unhandled grouping-task flux-policy." task-map))

          :else
          (fn [hash-group]
            (choose-f hash-group)))))


;; Implement GC replica id
;; Compare to next replica id, decide what to throw away
;; On stop, just clean up all, final compare should compare against nothing.

(defmethod extensions/peer-replica-view :default 
  [log entry old-replica new-replica diff old-view state peer-config]
  (let [peer-id (:id state)
        messenger (:messenger state)
        allocations (:allocations new-replica)
        allocated-job (common/peer->allocated-job allocations peer-id)
        task-id (:task allocated-job)
        job-id (:job allocated-job)]
    (if job-id
      (let [task (if (= task-id (:task-id old-view)) 
                   (:task old-view)
                   (extensions/read-chunk log :task task-id))
            _ (info "task is " task)
            catalog (if (= job-id (:job-id old-view)) 
                      (:catalog old-view)
                      (extensions/read-chunk log :catalog job-id))
            workflow (if (= job-id (:job-id old-view)) 
                      (:workflow old-view)
                      (extensions/read-chunk log :workflow job-id))
            root-task-ids (mapv
                             (fn [root-task]
                               (get-in new-replica [:task-name->id job-id root-task]))
                             (common/root-tasks workflow (:name task)))
            {:keys [peer-state]} new-replica
            receivable-peers (common/job-receivable-peers peer-state allocations job-id)
            _ (info "New peers " peer-id (vec (remove (or (:receivable-peers old-view) {}) receivable-peers)))
            _ (info "Delete peers " peer-id (vec (remove receivable-peers (or (:receivable-peers old-view) {}))))
            ;; TODO implement acking peer publications / task-id / peer-id
            slot-ids (get-in new-replica [:task-slot-ids job-id])
            peer-sites (:peer-sites new-replica)
            egress-ids (:egress-ids task)
            egress-sites (set (map peer-sites (distinct (mapcat receivable-peers (vals egress-ids)))))
            egress-publications (zipmap egress-sites (mapv aeron/new-publication egress-sites))
            _ (info "egress sites " egress-sites egress-publications)
            ;; Egress publications for messages, can choose any publication under a task for a message
            egress-pubs (->> (:egress-ids task)
                             (map (fn [[task-name task-id]]
                                    (let [;slot-id->peer-id (map-invert (get slot-ids task-id))
                                          egress-peers (receivable-peers task-id)
                                          publications (->> egress-peers 
                                                            (map peer-sites)
                                                            (map egress-publications)
                                                            vec)] 
                                      [task-id publications])))
                             (into {}))
            barrier-pubs (vals egress-publications)]
        (info "egress pubs " egress-pubs "barrier-pubs" barrier-pubs " root task ids " root-task-ids)
        (->PeerReplicaView egress-pubs barrier-pubs root-task-ids job-id task-id workflow catalog task))
      (->PeerReplicaView nil nil nil nil nil nil nil nil))))
