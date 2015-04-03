(ns onyx.scheduling.greedy-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :refer [select-job]]))

(defmethod select-job :onyx.job-scheduler/greedy
  [{:keys [args]} replica]
  (let [job (first (universally-executable-jobs replica))
        allocation (common/peer->allocated-job (:allocations replica) (:id args))]
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

(defmethod volunteer-via-new-job? :onyx.job-scheduler/greedy
  [old new diff state]
  (when (zero? (count (incomplete-jobs old)))
    (any-coverable-jobs? new)))

(defmethod volunteer-via-leave? :onyx.job-scheduler/greedy
  [old new diff state]
  (let [allocation (peer->allocated-job (:allocations new) (:id state))
        peer-counts (balance-jobs new)
        peers (get (job->peers new) (:job allocation))]
    (when (> (count peers) (get peer-counts (:job allocation)))
      (let [n (- (count peers) (get peer-counts (:job allocation)))
            peers-to-drop (drop-peers new (:job allocation) n)]
        (when (some #{(:id state)} (into #{} peers-to-drop))
          [{:fn :volunteer-for-task :args {:id (:id state)}}])))))

(defmethod volunteer-via-killed-job? :onyx.job-scheduler/greedy
  [old new diff state]
  (let [peers (apply concat (vals (get-in old [:allocations (first diff)])))]
    (when (some #{(:id state)} (into #{} peers))
      (any-coverable-jobs? new))))

(defmethod volunteer-via-sealed-output? :onyx.job-scheduler/greedy
  [old new diff state]
  (and (:job-completed? diff)
       (seq (incomplete-jobs new))
       (any-coverable-jobs? new)))

(defn has-peers-allocated? [replica job]
  (pos? (count (get-in replica [:allocations job]))))

(defmethod volunteer-via-accept? :onyx.job-scheduler/greedy
  [old new diff state]
  ; Greedy should preferentially allocate to the jobs that 
  ; already have peers, otherwise to first incomplete job.
  ; This ensures stable greedy allocation
  (let [incomplete (incomplete-jobs new)]
    (or (->> incomplete
             (filter (partial has-peers-allocated? new))
             (filter (partial job-coverable? new))
             first)
        (->> incomplete
             (filter (partial job-coverable? new))
             first))))

(defmethod reallocate-from-job? :onyx.job-scheduler/greedy
  [scheduler old new state]
  (not (seq (alive-jobs old (:jobs old)))))
