(ns onyx.log.commands.complete-job
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]
            [onyx.log.commands.common :as common]))

(s/defmethod extensions/apply-log-entry :complete-job :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [job (:job-id args)] 
    (if (some #{job} (:jobs replica)) 
      (let [peers (reduce into [] (vals (get-in replica [:allocations job])))]
        (-> replica
            (update-in [:jobs] (fn [coll] (remove (partial = job) coll)))
            (update-in [:jobs] vec)
            (update-in [:completed-jobs] vec)
            (update-in [:completed-jobs] conj job)
            (update-in [:coordinators] dissoc job)
            (update-in [:in->out] dissoc job)
            (update-in [:task-metadata] dissoc job)
            (update-in [:task-slot-ids] dissoc job)
            (update-in [:allocation-version] dissoc job)
            (update-in [:allocations] dissoc job)
            (reconfigure-cluster-workload replica)))
      replica)))

(s/defmethod extensions/replica-diff :complete-job :- ReplicaDiff
  [{:keys [args]} old new]
  {})

(s/defmethod extensions/reactions [:complete-job :peer] :- Reactions
  [{:keys [args message-id]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! [:complete-job :peer] :- State
  [{:keys [args message-id]} old new diff state]
  (common/start-new-lifecycle old new diff state :job-completed))
