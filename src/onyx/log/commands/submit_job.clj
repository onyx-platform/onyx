(ns onyx.log.commands.submit-job
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [taoensso.timbre :refer [info warn]]))

(defmulti job-scheduler-replica-update
  (fn [replica entry]
    (:job-scheduler replica)))

(defmethod job-scheduler-replica-update :onyx.job-scheduler/percentage
  [replica {:keys [args]}]
  (assoc-in replica [:percentages (:id args)] (:percentage args)))

(defmethod job-scheduler-replica-update :default
  [replica entry]
  replica)

(defmulti task-scheduler-replica-update
  (fn [replica entry]
    (:task-scheduler (:args entry))))

(defmethod task-scheduler-replica-update :onyx.task-scheduler/percentage
  [replica {:keys [args]}]
  (assoc-in replica [:task-percentages (:id args)] (:task-percentages args)))

(defmethod task-scheduler-replica-update :default
  [replica entry]
  replica)

(s/defmethod extensions/apply-log-entry :submit-job :- Replica
  [{:keys [args] :as entry} :- LogEntry replica]
  (try
    (if (get (union (set (:jobs replica))
                    (set (:killed-jobs replica))
                    (set (:completed-jobs replica)))
             (:id args))
      (do (info (format "Job ID %s has already been submitted, and will not be scheduled again." (:id args)))
          replica)
      (-> replica
          (update-in [:jobs] conj (:id args))
          (update-in [:jobs] vec)
          (assoc-in [:task-schedulers (:id args)] (:task-scheduler args))
          (assoc-in [:tasks (:id args)] (vec (:tasks args)))
          (assoc-in [:allocations (:id args)] {})
          (assoc-in [:saturation (:id args)] (:saturation args))
          (assoc-in [:task-saturation (:id args)] (:task-saturation args))
          (assoc-in [:flux-policies (:id args)] (:flux-policies args))
          (assoc-in [:min-required-peers (:id args)] (:min-required-peers args))
          (assoc-in [:input-tasks (:id args)] (vec (:inputs args)))
          (assoc-in [:output-tasks (:id args)] (vec (:outputs args)))
          (assoc-in [:exempt-tasks (:id args)] (vec (:exempt-tasks args)))
          (assoc-in [:acker-percentage (:id args)] (:acker-percentage args))
          (assoc-in [:acker-exclude-inputs (:id args)] (:acker-exclude-inputs args))
          (assoc-in [:acker-exclude-outputs (:id args)] (:acker-exclude-outputs args))
          (assoc-in [:required-tags (:id args)] (:required-tags args))
          (job-scheduler-replica-update entry)
          (task-scheduler-replica-update entry)
          (reconfigure-cluster-workload)))
    (catch Throwable e
      (warn e)
      replica)))

(s/defmethod extensions/replica-diff :submit-job :- ReplicaDiff
  [{:keys [args]} old new]
  (if (and (not (some #{(:id args)} (:jobs old)))
           (some #{(:id args)} (:jobs new)))
    {:job (:id args)}
    {}))

(s/defmethod extensions/reactions [:submit-job :peer] :- Reactions
  [{:keys [args] :as entry} old new diff state]
  [])

(s/defmethod extensions/fire-side-effects! [:submit-job :peer] :- State
  [entry old new diff state]
  (common/start-new-lifecycle old new diff state :peer-reallocated))
