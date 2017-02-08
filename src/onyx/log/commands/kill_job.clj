(ns onyx.log.commands.kill-job
  (:require [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :refer [peer->allocated-job] :as common]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [taoensso.timbre :refer [warn]]))

(defn apply-kill-job [replica job-id]
  (if-not (some #{job-id} (:killed-jobs replica))
    (let [peers (mapcat identity (vals (get-in replica [:allocations job-id])))]
      (-> replica
          (update-in [:jobs] (fn [coll] (remove (partial = job-id) coll)))
          (update-in [:jobs] vec)
          (update-in [:killed-jobs] conj job-id)
          (update-in [:killed-jobs] vec)
          (update-in [:allocations] dissoc job-id)
          (update-in [:coordinators] dissoc job-id)
          (update-in [:task-metadata] dissoc job-id)
          (update-in [:task-slot-ids] dissoc job-id)
          (update-in [:in->out] dissoc job-id)
          (reconfigure-cluster-workload replica)))
    replica))

(defn enforce-flux-policy [replica id]
  (let [allocation (peer->allocated-job (:allocations replica) id)]
    (if (= (get-in replica [:flux-policies (:job allocation) (:task allocation)]) :kill)
      (apply-kill-job replica (:job allocation))
      replica)))

(s/defmethod extensions/apply-log-entry :kill-job :- Replica
  [{:keys [args]} :- LogEntry replica :- Replica]
  (try
    (apply-kill-job replica (:job args))
    (catch Throwable e
      (warn e)
      replica)))

(s/defmethod extensions/replica-diff :kill-job :- ReplicaDiff
  [entry old new]
  (second (diff (into #{} (:killed-jobs old)) (into #{} (:killed-jobs new)))))

(s/defmethod extensions/fire-side-effects! [:kill-job :peer] :- State
  [{:keys [args]} old new diff state]
  (common/start-new-lifecycle old new diff state :job-killed))
