(ns onyx.log.commands.leave-cluster
  (:require [clojure.core.async :refer [chan go >! <! >!! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]
            [onyx.log.commands.kill-job :as kill]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(defn deallocated-replica [args replica]
  (let [{:keys [id group-id]} args]
    (assert id)
    (assert group-id)
    (-> replica
        (kill/enforce-flux-policy id)
        (update-in [:peers] (partial remove #(= % id)))
        (update-in [:peers] vec)
        (update-in [:orphaned-peers] (fn [orphaned] 
                                       (if-let [group-peers (get orphaned group-id)]
                                         (assoc orphaned group-id (vec (remove #(= % id) group-peers)))
                                         orphaned)))
        (update-in [:peer-state] dissoc id)
        (update-in [:peer-sites] dissoc id)
        (update-in [:peer-tags] dissoc id)
        (update-in [:groups-index] (fn [groups-index]
                                     (if-let [idx (get groups-index group-id)]
                                       (assoc groups-index group-id (disj idx id))
                                       groups-index)))
        (update-in [:groups-reverse-index] dissoc id)
        (common/remove-peers id))))

(s/defmethod extensions/apply-log-entry :leave-cluster :- Replica
  [{:keys [args]} :- LogEntry replica]
  (reconfigure-cluster-workload (deallocated-replica args replica)))

(s/defmethod extensions/replica-diff :leave-cluster :- ReplicaDiff
  [{:keys [args]} old new]
  {:died (:id args)})

(s/defmethod extensions/reactions [:leave-cluster :peer] :- Reactions
  [{:keys [args]} old new diff state]
  (when (and (= (:id state) (:id args))
             (:restart? args))
    [{:fn :add-virtual-peer
      :args {:id (:restarted-id args)
             :group-id (:group-id state)
             :peer-site (:peer-site state)
             :tags (:onyx.peer/tags (:opts state))}}]))

(s/defmethod extensions/fire-side-effects! [:leave-cluster :peer] :- State
  [{:keys [args]} old new diff state]
  (when (= (:id state) (:id args))
    ;; TODO, possibly allow quick reboot here if this is us
    )
  (common/start-new-lifecycle old new diff state :peer-reallocated))
