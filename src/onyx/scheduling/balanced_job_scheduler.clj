(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :refer [select-job]]))

(defn find-job-needing-peers [replica candidates]
  (let [balanced (balance-jobs replica)
        counts (job->peers replica)]
    (reduce
     (fn [default job]
       (when (< (count (get counts job)) (get balanced job 0))
         (reduced job)))
     nil
     candidates)))

(defn round-robin-next-job [replica candidates]
  (let [counts (job->peers replica)]
    (->> candidates
         (reduce #(conj %1 {:job %2 :n (count (get counts %2))}) [])
         (sort-by :n)
         (first)
         :job)))

(defn saturated-cluster? [replica]
  (let [balanced (balance-jobs replica)
        counts (job->peers replica)]
    (and (= balanced (into {} (map (fn [[job peers]] {job (count peers)}) counts)))
         (= (apply + (vals balanced)) (count (:peers replica))))))

(defmethod select-job :onyx.job-scheduler/balanced
  [{:keys [args]} replica]
  (if-not (common/saturated-cluster? replica)
    (let [candidates (universally-executable-jobs replica)
          allocation (common/peer->allocated-job (:allocations replica) (:id args))
          job (or (common/find-job-needing-peers replica candidates)
                  (common/round-robin-next-job replica candidates))]
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
        replica))
    replica))

(defmethod volunteer-via-new-job? :onyx.job-scheduler/balanced
  [old new diff state]
  (let [allocations (balance-jobs new)]
    (every?
     (fn [job]
       (let [n-tasks (count (get-in new [:tasks job]))]
         (>= (get allocations job) n-tasks)))
     (incomplete-jobs new))))

(defmethod volunteer-via-leave? :onyx.job-scheduler/balanced
  [old new diff state]
  (let [allocations (balance-jobs new)
        allocation (peer->allocated-job (:allocations new) (:id state))]
    (when allocation
      (let [n-required (get allocations (:job allocation))
            n-actual (count (apply concat (vals (get-in new [:allocations (:job allocation)]))))]
        (> n-actual n-required)))))

(defmethod volunteer-via-killed-job? :onyx.job-scheduler/balanced
  [old new diff state]
  (seq (incomplete-jobs new)))

(defmethod volunteer-via-sealed-output? :onyx.job-scheduler/balanced
  [old new diff state]
  (seq (incomplete-jobs new)))

(defmethod volunteer-via-accept? :onyx.job-scheduler/balanced
  [old new diff state]
  (let [allocation (peer->allocated-job (:allocations new) (:id state))]
    (and (seq (incomplete-jobs new))
         (nil? (:job allocation))
         (every? (partial job-coverable? new) (incomplete-jobs new)))))

(defmethod reallocate-from-job? :onyx.job-scheduler/balanced
  [scheduler old new state]
  (boolean
    (if-let [allocation (peer->allocated-job (:allocations new) (:id state))]
      (let [peer-counts (balance-jobs new)
            peers (get (job->peers new) (:job allocation))]
        (when (> (count peers) (get peer-counts (:job allocation)))
          (let [n (- (count peers) (get peer-counts (:job allocation)))
                peers-to-drop (drop-peers new (:job allocation) n)]
            (some #{(:id state)} peers-to-drop))))
      true)))
