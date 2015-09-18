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

;; Pulled this out of the defmethod because it's reused across other log entries.
(defn apply-kill-job [replica job-id]
  (if-not (some #{job-id} (:killed-jobs replica))
    (let [peers (mapcat identity (vals (get-in replica [:allocations job-id])))]
      (-> replica
          (update-in [:jobs] (fn [coll] (remove (partial = job-id) coll)))
          (update-in [:jobs] vec)
          (update-in [:killed-jobs] conj job-id)
          (update-in [:killed-jobs] vec)
          (update-in [:allocations] dissoc job-id)
          (update-in [:task-metadata] dissoc job-id)
          (update-in [:ackers] dissoc job-id)
          (update-in [:task-slot-ids] dissoc job-id)
          (update-in [:peer-state] merge (into {} (map (fn [p] {p :idle}) peers)))
          (reconfigure-cluster-workload)))
    replica))

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

(s/defmethod extensions/reactions :kill-job :- Reactions
  [{:keys [args]} old new diff state]
  [])

(s/defmethod extensions/fire-side-effects! :kill-job :- State
  [{:keys [args]} old new diff state]
  (common/start-new-lifecycle old new diff state))
