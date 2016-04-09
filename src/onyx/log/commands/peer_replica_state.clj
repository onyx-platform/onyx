(ns onyx.log.commands.peer-replica-state
  (:require [clojure.set :refer [intersection union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.messaging.aeron :as aeron]
            [taoensso.timbre :refer [info warn]]
            [onyx.static.planning :as planning]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

; (defn peer-site [peer-replica-state peer-id]
;   (get (:peer-sites @peer-replica-state) peer-id))

(defrecord PeerReplicaView
  [site->publication task-id->publications barrier-publications root-task-ids job-id task-id workflow catalog task])

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

(defn remove-publication [{:keys [pub conn]}]
  (info "stopping pub " pub)
  (.close pub)
  (.close conn))

(defn stop-task-state [task-state]
  (info "removing in stop task state " (vec (vals (:site->publication task-state))))
  (run! remove-publication 
        (vals (:site->publication task-state))))

;; Implement GC replica id
;; Compare to next replica id, decide what to throw away
;; On stop, just clean up all, final compare should compare against nothing.

(defn task-egress-publications [egress-tasks receivable-peers peer-sites site->publication]
  (->> egress-tasks
       (map (fn [[task-name task-id]]
              (let [;slot-id->peer-id (map-invert (get slot-ids task-id))
                    egress-peers (receivable-peers task-id)
                    publications (->> egress-peers 
                                      (map (comp :pub site->publication peer-sites))
                                      vec)] 
                [task-id publications])))
       (into {})))

(defmethod extensions/peer-replica-state :default 
  [log entry old-replica new-replica diff old-peer-replica-state state peer-config]
  (let [peer-id (:id state)
        messenger (:messenger state)
        allocations (:allocations new-replica)
        allocated-job (common/peer->allocated-job allocations peer-id)
        task-id (:task allocated-job)
        job-id (:job allocated-job)]
    (if job-id
      (let [task (if (= task-id (:task-id old-peer-replica-state)) 
                   (:task old-peer-replica-state)
                   (extensions/read-chunk log :task task-id))
            catalog (if (= job-id (:job-id old-peer-replica-state)) 
                      (:catalog old-peer-replica-state)
                      (extensions/read-chunk log :catalog job-id))
            workflow (if (= job-id (:job-id old-peer-replica-state)) 
                       (:workflow old-peer-replica-state)
                       (extensions/read-chunk log :workflow job-id))
            root-task-ids (mapv
                            (fn [root-task]
                              (get-in new-replica [:task-name->id job-id root-task]))
                            (common/root-tasks workflow (:name task)))
            {:keys [peer-state]} new-replica
            receivable-peers (common/job-receivable-peers peer-state allocations job-id)
            ;; TODO implement acking peer publications / task-id / peer-id
            slot-ids (get-in new-replica [:task-slot-ids job-id])
            peer-sites (:peer-sites new-replica)
            egress-ids (:egress-ids task)
            egress-sites (set (map peer-sites (distinct (mapcat receivable-peers (vals egress-ids)))))
            old-egress-sites (set (keys (:site->publication old-peer-replica-state)))
            _ (info "old egress sites " old-egress-sites)
            new-egress-sites (difference egress-sites old-egress-sites)
            stale-egress-sites (difference old-egress-sites egress-sites)
            _ (info (:id state) (:message-id entry) "new egress sites " new-egress-sites "stale " stale-egress-sites "site to pub" (:site->publication old-peer-replica-state))
            cont-egress-sites (intersection egress-sites old-egress-sites)
            cont-site->publication (select-keys (:site->publication old-peer-replica-state) cont-egress-sites)
            new-site->publication (zipmap new-egress-sites (mapv aeron/new-publication new-egress-sites))
            site->publication (merge cont-site->publication new-site->publication) 
            _ (assert (= (count site->publication) (+ (count cont-site->publication) (count new-site->publication))))
            task-id->publications (task-egress-publications egress-ids receivable-peers peer-sites site->publication)
            barrier-publications (mapv :pub (vals site->publication))
            stale-publications (map (:site->publication old-peer-replica-state) stale-egress-sites)]
        ;; Cleanup stale egress sites
        (info "removing stale publication " (vec stale-publications))
        (run! remove-publication stale-publications)
        (info "egress publications " egress-sites)
        (info "site->publication " site->publication "task-id->publications" task-id->publications "barrier-publications" barrier-publications)
        (->PeerReplicaView site->publication task-id->publications barrier-publications root-task-ids job-id task-id workflow catalog task))
      (do
        (stop-task-state old-peer-replica-state)
        (->PeerReplicaView nil nil nil nil nil nil nil nil nil)))))
