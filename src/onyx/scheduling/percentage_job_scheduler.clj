(ns onyx.scheduling.percentage-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :refer [select-job]]))

(defmethod select-job :onyx.job-scheduler/percentage
  [{:keys [args]} replica]
  (let [candidates (universally-executable-jobs replica)
        balanced (common/percentage-balanced-workload replica)
        allocation (common/peer->allocated-job (:allocations replica) (:id args))
        job
        (reduce
         (fn [_ job]
           (let [required-count (:allocation (get balanced job))
                 actual-count (count (get (common/job->peers replica) job))]
             (when (< actual-count required-count)
               (reduced job))))
         nil
         candidates)]
    (if job
      (if-let [task (select-task replica job (:id args))]
        (if (or (not= task (:task allocation))
                (not= job (:job allocation)))
          (-> replica
              (common/remove-peers args)
              (update-in [:allocations job task] conj (:id args))
              (update-in [:allocations job task] vec)
              (assoc-in [:peer-state (:id args)] :warming-up)
              (offer-acker job task args))
          replica)
        replica)
      replica)))

(defmethod volunteer-via-new-job? :onyx.job-scheduler/percentage
  [old new diff state]
  (let [allocations (percentage-balanced-workload new)]
    (every?
     (fn [job]
       (let [n-tasks (count (get-in new [:tasks job]))]
         (>= (:allocation (get allocations job)) n-tasks)))
     (incomplete-jobs new))))

(defmethod volunteer-via-leave? :onyx.job-scheduler/percentage
  [old new diff state]
  (let [allocations (percentage-balanced-workload new)
        allocation (peer->allocated-job (:allocations new) (:id state))]
    (when (and allocation (seq (incomplete-jobs new)))
      (let [n-required (:allocation (get allocations (:job allocation)))
            n-actual (count (apply concat (vals (get-in new [:allocations (:job allocation)]))))]
        (> n-actual n-required)))))

(defmethod volunteer-via-killed-job? :onyx.job-scheduler/percentage
  [old new diff state]
  (seq (incomplete-jobs new)))

(defmethod volunteer-via-sealed-output? :onyx.job-scheduler/percentage
  [old new diff state]
  (seq (incomplete-jobs new)))

(defmethod volunteer-via-accept? :onyx.job-scheduler/percentage
  [old new diff state]
  (and (nil? (:job state))
       (every? (partial job-coverable? new) (incomplete-jobs new))))

(defmethod reallocate-from-job? :onyx.job-scheduler/percentage
  [scheduler old new state]
  (if-let [allocation (peer->allocated-job (:allocations new) (:id state))]
    (let [balanced (percentage-balanced-workload new)
          peer-counts (:allocation (get balanced (:job allocation)))
          peers (get (job->peers new) (:job allocation))]
      (when (> (count peers) peer-counts)
        (let [n (- (count peers) peer-counts)
              peers-to-drop (drop-peers new (:job allocation) n)]
          (when (some #{(:id state)} peers-to-drop)
            true))))
    true))
