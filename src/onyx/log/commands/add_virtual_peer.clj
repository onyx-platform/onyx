(ns onyx.log.commands.add-virtual-peer
  (:require [onyx.extensions :as extensions]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.commands.common :as common]
            [schema.core :as s]))

(defn add-site-acker [replica {:keys [id peer-site]}]
  (assert (:messaging replica) ":messaging key missing in replica, cannot continue")
  (-> replica
      (assoc-in [:peer-sites id]
                (merge
                  peer-site
                  (extensions/assign-acker-resources replica
                                                     id
                                                     peer-site
                                                     (:peer-sites replica))))))

(defn register-peer-info [replica args]
  (-> replica
      (update-in [:groups-index (:group-id args)] (fnil conj #{}) (:id args))
      (assoc-in [:peer-state (:id args)] :idle)
      (assoc-in [:peer-tags (:id args)] (:tags args))
      (add-site-acker args)))

(s/defmethod extensions/apply-log-entry :add-virtual-peer :- Replica
  [{:keys [args]} :- LogEntry replica :- Replica]
  (if (some #{(:group-id args)} (:groups replica))
    (-> replica
        (update-in [:peers] conj (:id args))
        (update-in [:peers] vec)
        (register-peer-info args)
        (reconfigure-cluster-workload))
    (-> replica
        (update-in [:orphaned-peers] conj (:id args))
        (update-in [:orphaned-peers] vec)
        (register-peer-info args))))

(s/defmethod extensions/replica-diff :add-virtual-peer :- ReplicaDiff
  [{:keys [args]} old new]
  {:virtual-peer-id (:id args)})

(s/defmethod extensions/reactions :add-virtual-peer :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(defn register-acker [state diff new]
  (when (= (:id state) (:subject diff))
    (extensions/register-acker
     (:messenger state)
     (get-in new [:peer-sites (:id state)]))))

(s/defmethod extensions/fire-side-effects! :add-virtual-peer :- State
  [{:keys [args message-id]} old new diff state]
  (when (= (:id args) (:id state))
    (register-acker state diff new))
  (common/start-new-lifecycle old new diff state :peer-reallocated))


;;; TODO - [x] index vpeer -> group id and group id -> vpeer, probably useful
;;; TODO - [x] add acker site on join
;;; TODO - [x] handle group not joined yet
;;; TODO - [x] joining peer should supply its tags
;;; TODO - [x] make group-leave-cluster
;;; TODO - [x] switch monitoring for peer-join-cluster to group-join-cluster, add peer-join attempts too, & accept, & notify
;;; TODO - [x] handle sealing in, move from group into peer leaving
;;; TODO - [ ] make tasklifecycle rebootable by itself
