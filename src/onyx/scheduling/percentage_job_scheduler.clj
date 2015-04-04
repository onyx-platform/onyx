(ns onyx.scheduling.percentage-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(comment
  

  (defn percentage-balanced-workload [replica]
    (let [n-peers (count (:peers replica))
          sorted-jobs (sort-jobs-by-pct replica)
          jobs-to-use (maximum-jobs-to-use sorted-jobs)
          init-allocations (min-allocations jobs-to-use n-peers)
          init-usage (apply + (map :allocation init-allocations))
          left-over-peers (- n-peers init-usage)
          with-leftovers (update-in init-allocations [0 :allocation] + left-over-peers)]
      (into {} (map (fn [j] {(:job j) j}) with-leftovers))))

  (defmethod cjs/select-job :onyx.job-scheduler/percentage
    [{:keys [args]} replica]
    (let [candidates (cjs/universally-executable-jobs replica)
          balanced (percentage-balanced-workload replica)
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
        replica)))

  (defmethod cjs/volunteer-via-new-job? :onyx.job-scheduler/percentage
    [old new diff state]
    (let [allocations (percentage-balanced-workload new)]
      (every?
       (fn [job]
         (let [n-tasks (count (get-in new [:tasks job]))]
           (>= (:allocation (get allocations job)) n-tasks)))
       (common/incomplete-jobs new))))

  (defmethod cjs/volunteer-via-leave? :onyx.job-scheduler/percentage
    [old new diff state]
    (let [allocations (percentage-balanced-workload new)
          allocation (common/peer->allocated-job (:allocations new) (:id state))]
      (when (and allocation (seq (common/incomplete-jobs new)))
        (let [n-required (:allocation (get allocations (:job allocation)))
              n-actual (count (apply concat (vals (get-in new [:allocations (:job allocation)]))))]
          (> n-actual n-required)))))

  (defmethod cjs/volunteer-via-killed-job? :onyx.job-scheduler/percentage
    [old new diff state]
    (seq (common/incomplete-jobs new)))

  (defmethod cjs/volunteer-via-sealed-output? :onyx.job-scheduler/percentage
    [old new diff state]
    (seq (common/incomplete-jobs new)))

  (defmethod cjs/volunteer-via-accept? :onyx.job-scheduler/percentage
    [old new diff state]
    (and (nil? (:job state))
         (every? (partial cjs/job-coverable? new) (common/incomplete-jobs new))))

  (defmethod cjs/reallocate-from-job? :onyx.job-scheduler/percentage
    [scheduler old new state]
    (if-let [allocation (common/peer->allocated-job (:allocations new) (:id state))]
      (let [balanced (percentage-balanced-workload new)
            peer-counts (:allocation (get balanced (:job allocation)))
            peers (get (common/job->peers new) (:job allocation))]
        (when (> (count peers) peer-counts)
          (let [n (- (count peers) peer-counts)
                peers-to-drop (cts/drop-peers new (:job allocation) n)]
            (when (some #{(:id state)} peers-to-drop)
              true))))
      true)))

(defn sort-jobs-by-pct [replica]
    (let [indexed
          (map-indexed
           (fn [k j]
             {:position k :job j :pct (get-in replica [:percentages j])})
           (reverse (:jobs replica)))]
      (reverse (sort-by (juxt :pct :position) indexed))))

(defn min-allocations [jobs n-peers]
  (mapv
   (fn [job]
     (let [n (int (Math/floor (* (* 0.01 (:pct job)) n-peers)))]
       (assoc job :capacity n)))
   jobs))

(defn maximum-jobs-to-use [jobs]
  (reduce
   (fn [all {:keys [pct] :as job}]
     (let [sum (apply + (map :pct all))]
       (if (<= (+ sum pct) 100)
         (conj all job)
         (reduced all))))
   []
   jobs))

(defmethod cjs/job-offer-n-peers :onyx.job-scheduler/percentage
  [replica]
  (let [n-peers (count (:peers replica))
        sorted-jobs (sort-jobs-by-pct replica)
        jobs-to-use (maximum-jobs-to-use sorted-jobs)
        init-allocations (min-allocations jobs-to-use n-peers)]
    (into {} (map (fn [j] {(:job j) (:capacity j)}) init-allocations))))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/percentage
  [replica jobs n]
  ;; We can get away with using the exact same algorithm as the
  ;; Balanced job scheduler.
  (cjs/claim-spare-peers
   (assoc replica :job-scheduler :onyx.job-scheduler/balanced) jobs n))
