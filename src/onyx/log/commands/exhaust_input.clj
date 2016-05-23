(ns onyx.log.commands.exhaust-input
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]))

(s/defmethod extensions/apply-log-entry :exhaust-input :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [job-id (:job args)
        new (update-in replica [:exhausted-inputs job-id] union #{(:task args)})]
    (if (and (= (into #{} (get-in new [:input-tasks job-id]))
                (into #{} (get-in new [:exhausted-inputs job-id])))
             (not (some #{job-id} (:completed-jobs replica))))
      (let [peers (reduce into [] (vals (get-in new [:allocations job-id])))]
        (-> new
            (update-in [:exhausted-inputs] dissoc job-id)
            (update-in [:jobs] (fn [coll] (remove (partial = job-id) coll)))
            (update-in [:jobs] vec)
            (update-in [:completed-jobs] conj job-id)
            (update-in [:completed-jobs] vec)
            (update-in [:task-metadata] dissoc job-id)
            (update-in [:task-slot-ids] dissoc job-id)
            (update-in [:allocations] dissoc job-id)
            (update-in [:peer-state] merge (into {} (map (fn [p] {p :idle}) peers)))
            (reconfigure-cluster-workload)))
      new)))

(s/defmethod extensions/replica-diff :exhaust-input :- ReplicaDiff
  [{:keys [args]} old new]
  {:job (:job args) :task (:task args)})

(s/defmethod extensions/reactions :exhaust-input :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :exhaust-input :- State
  [{:keys [args message-id]} old new diff state]
  (common/start-new-lifecycle old new diff state :job-completed))
