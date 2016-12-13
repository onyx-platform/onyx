(ns onyx.log.commands.add-virtual-peer
  (:require [onyx.extensions :as extensions]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.commands.common :as common]
            [taoensso.timbre :as timbre :refer [info]]
            [schema.core :as s]))

(defn register-peer-info [replica args]
  (-> replica
      (update-in [:groups-index (:group-id args)] (fnil conj #{}) (:id args))
      (assoc-in [:groups-reverse-index (:id args)] (:group-id args))
      (assoc-in [:peer-sites (:id args)] (:peer-site args))
      (assoc-in [:peer-tags (:id args)] (:tags args))))

(s/defmethod extensions/apply-log-entry :add-virtual-peer :- Replica
  [{:keys [args]} :- LogEntry replica :- Replica]
  (cond (get-in replica [:left (:group-id args)])
        replica
        (some #{(:group-id args)} (:groups replica))
        (-> replica
            (update-in [:peers] conj (:id args))
            (update-in [:peers] vec)
            (register-peer-info args)
            (reconfigure-cluster-workload replica))
        :else 
        (-> replica
            (update-in [:orphaned-peers (:group-id args)] (fnil conj []) (:id args))
            (register-peer-info args))))

(s/defmethod extensions/replica-diff :add-virtual-peer :- ReplicaDiff
  [{:keys [args]} old new]
  (if-not (= old new) 
    {:virtual-peer-id (:id args)}))

(s/defmethod extensions/fire-side-effects! [:add-virtual-peer :peer] :- State
  [{:keys [args message-id] :as entry} old new diff state]
  (common/start-new-lifecycle old new diff state :peer-reallocated))
