(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defn find-job-needing-peers [replica candidates]
  (let [balanced (cjs/balance-jobs replica)
        counts (common/job->peers replica)]
    (reduce
     (fn [default job]
       (when (< (count (get counts job)) (get balanced job 0))
         (reduced job)))
     nil
     candidates)))

(defn round-robin-next-job [replica candidates]
  (let [counts (common/job->peers replica)]
    (->> candidates
         (reduce #(conj %1 {:job %2 :n (count (get counts %2))}) [])
         (sort-by :n)
         (first)
         :job)))

(defn saturated-cluster? [replica]
  (let [balanced (cjs/balance-jobs replica)
        counts (common/job->peers replica)]
    (and (= balanced (into {} (map (fn [[job peers]] {job (count peers)}) counts)))
         (= (apply + (vals balanced)) (count (:peers replica))))))

(defmethod cjs/select-job :onyx.job-scheduler/balanced
  [{:keys [args]} replica]
  (if-not (saturated-cluster? replica)
    (let [candidates (cjs/universally-executable-jobs replica)
          allocation (common/peer->allocated-job (:allocations replica) (:id args))
          job (or (find-job-needing-peers replica candidates)
                  (round-robin-next-job replica candidates))]
      (if job
        (if-let [task (cts/select-task replica job (:id args))]
          (if (or (not= task (:task allocation))
                  (not= job (:job allocation)))
            (-> replica
                (common/remove-peers args)
                (update-in [:allocations job task] conj (:id args))
                (update-in [:allocations job task] vec)
                (assoc-in [:peer-state (:id args)] :warming-up)
                (cjs/offer-acker job task args))
            replica)
          replica)
        replica))
    replica))

(defmethod cjs/volunteer-via-new-job? :onyx.job-scheduler/balanced
  [old new diff state]
  (let [allocations (cjs/balance-jobs new)]
    (every?
     (fn [job]
       (let [n-tasks (count (get-in new [:tasks job]))]
         (>= (get allocations job) n-tasks)))
     (common/incomplete-jobs new))))

(defmethod cjs/volunteer-via-leave? :onyx.job-scheduler/balanced
  [old new diff state]
  (let [allocations (cjs/balance-jobs new)
        allocation (common/peer->allocated-job (:allocations new) (:id state))]
    (when allocation
      (let [n-required (get allocations (:job allocation))
            n-actual (count (apply concat (vals (get-in new [:allocations (:job allocation)]))))]
        (> n-actual n-required)))))

(defmethod cjs/volunteer-via-killed-job? :onyx.job-scheduler/balanced
  [old new diff state]
  (seq (common/incomplete-jobs new)))

(defmethod cjs/volunteer-via-sealed-output? :onyx.job-scheduler/balanced
  [old new diff state]
  (seq (common/incomplete-jobs new)))

(defmethod cjs/volunteer-via-accept? :onyx.job-scheduler/balanced
  [old new diff state]
  (let [allocation (common/peer->allocated-job (:allocations new) (:id state))]
    (and (seq (common/incomplete-jobs new))
         (nil? (:job allocation))
         (every? (partial cjs/job-coverable? new) (common/incomplete-jobs new)))))

(defmethod cjs/reallocate-from-job? :onyx.job-scheduler/balanced
  [scheduler old new state]
  (boolean
    (if-let [allocation (common/peer->allocated-job (:allocations new) (:id state))]
      (let [peer-counts (cjs/balance-jobs new)
            peers (get (common/job->peers new) (:job allocation))]
        (when (> (count peers) (get peer-counts (:job allocation)))
          (let [n (- (count peers) (get peer-counts (:job allocation)))
                peers-to-drop (cts/drop-peers new (:job allocation) n)]
            (some #{(:id state)} peers-to-drop))))
      true)))
