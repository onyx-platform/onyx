(ns onyx.scheduling.greedy-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defn job-coverable? [replica job]
  (let [tasks (get-in replica [:tasks job])]
    (>= (count (get-in replica [:peers])) (count tasks))))

(defmethod cjs/job-offer-n-peers :onyx.job-scheduler/greedy
  [replica]
  (let [[active & passive] (:jobs replica)
        coverable? (job-coverable? replica active)
        n (if coverable? (:peers replica) 0)]
    (merge {active (count n)} (zipmap passive (repeat 0)))))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/greedy
  [replica jobs n]
  ;; This is a trivial case. A Greedy job scheduler has already offered
  ;; all the peers to the first available job. If there are extra peers,
  ;; they would simply be offered back to the same job, which would refuse
  ;; them. Return the same job claims since nothing will change.
  ;; 
  jobs)




;; (defmethod cjs/select-job :onyx.job-scheduler/greedy
;;   [{:keys [args]} replica]
;;   (let [job (first (cjs/universally-executable-jobs replica))
;;         allocation (common/peer->allocated-job (:allocations replica) (:id args))]
;;     (if job
;;       (if-let [task (cts/select-task replica job (:id args))]
;;         (if (or (not= task (:task allocation))
;;                 (not= job (:job allocation)))
;;           (-> replica
;;               (common/remove-peers args)
;;               (update-in [:allocations job task] conj (:id args))
;;               (update-in [:allocations job task] vec)
;;               (assoc-in [:peer-state (:id args)] :warming-up)
;;               (cjs/offer-acker job task args))
;;           replica)
;;         replica)
;;       replica)))

;; (defmethod cjs/volunteer-via-new-job? :onyx.job-scheduler/greedy
;;   [old new diff state]
;;   (when (zero? (count (common/incomplete-jobs old)))
;;     (any-coverable-jobs? new)))

;; (defmethod cjs/volunteer-via-leave? :onyx.job-scheduler/greedy
;;   [old new diff state]
;;   (let [allocation (common/peer->allocated-job (:allocations new) (:id state))
;;         peer-counts (cjs/balance-jobs new)
;;         peers (get (common/job->peers new) (:job allocation))]
;;     (when (> (count peers) (get peer-counts (:job allocation)))
;;       (let [n (- (count peers) (get peer-counts (:job allocation)))
;;             peers-to-drop (cts/drop-peers new (:job allocation) n)]
;;         (when (some #{(:id state)} (into #{} peers-to-drop))
;;           (do ;; SCHEDULER TODO: << Removed volunteer >>
;;             nil))))))

;; (defmethod cjs/volunteer-via-killed-job? :onyx.job-scheduler/greedy
;;   [old new diff state]
;;   (let [peers (apply concat (vals (get-in old [:allocations (first diff)])))]
;;     (when (some #{(:id state)} (into #{} peers))
;;       (any-coverable-jobs? new))))

;; (defmethod cjs/volunteer-via-sealed-output? :onyx.job-scheduler/greedy
;;   [old new diff state]
;;   (and (:job-completed? diff)
;;        (seq (common/incomplete-jobs new))
;;        (any-coverable-jobs? new)))

;; (defn has-peers-allocated? [replica job]
;;   (pos? (count (get-in replica [:allocations job]))))

;; (defmethod cjs/volunteer-via-accept? :onyx.job-scheduler/greedy
;;   [old new diff state]
;;   ; Greedy should preferentially allocate to the jobs that 
;;   ; already have peers, otherwise to first incomplete job.
;;   ; This ensures stable greedy allocation
;;   (let [incomplete (common/incomplete-jobs new)]
;;     (or (->> incomplete
;;              (filter (partial has-peers-allocated? new))
;;              (filter (partial cjs/job-coverable? new))
;;              first)
;;         (->> incomplete
;;              (filter (partial cjs/job-coverable? new))
;;              first))))

;; (defmethod cjs/reallocate-from-job? :onyx.job-scheduler/greedy
;;   [scheduler old new state]
;;   (not (seq (cjs/alive-jobs old (:jobs old)))))
