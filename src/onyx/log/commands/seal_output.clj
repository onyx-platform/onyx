(ns onyx.log.commands.seal-output
  (:require [clojure.set :refer [union]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [taoensso.timbre :refer [warn fatal info]]))

(defn all-outputs-sealed? [replica job]
  (let [all (get-in replica [:output-tasks job])
        sealed (get-in replica [:sealed-outputs job])]
    (= (into #{} all) (into #{} sealed))))

(s/defmethod extensions/apply-log-entry :seal-output :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [new (update-in replica [:sealed-outputs (:job args)] union #{(:task args)})]
    (if (all-outputs-sealed? new (:job args))
      (let [peers (reduce into [] (vals (get-in replica [:allocations (:job args)])))]
        (-> new
            (update-in [:exhausted-inputs] dissoc (:job args))
            (update-in [:sealed-outputs] dissoc (:job args))
            (update-in [:jobs] (fn [coll] (remove (partial = (:job args)) coll)))
            (update-in [:jobs] vec)
            (update-in [:completed-jobs] conj (:job args))
            (update-in [:completed-jobs] vec)
            (update-in [:task-metadata] dissoc (:job args))
            (update-in [:task-slot-ids] dissoc (:job args))
            (update-in [:allocations] dissoc (:job args))
            (update-in [:peer-state] merge (into {} (map (fn [p] {p :idle}) peers)))
            (reconfigure-cluster-workload)))
      new)))

(s/defmethod extensions/replica-diff :seal-output :- ReplicaDiff
  [{:keys [args]} old new]
  {:job-completed? (not= (get-in old [:allocations (:job args)])
                         (get-in new [:allocations (:job args)]))
   :job (:job args)})

(s/defmethod extensions/reactions :seal-output :- Reactions
  [{:keys [args]} old new diff state]
  [])

(s/defmethod extensions/fire-side-effects! :seal-output :- State
  [{:keys [args]} old new diff state]
  (common/start-new-lifecycle old new diff state :job-completed))
