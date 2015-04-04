(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(comment
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
       true))))

(defn job-coverable? [replica job n]
  (let [tasks (get-in replica [:tasks job])]
    (>= n (count tasks))))

(defn allocate-peers [{:keys [jobs peers] :as replica}]
  (loop [results {}]
    (let [j (count jobs)
          p (count peers)
          min-peers (int (/ p j))
          n (rem p j)
          max-peers (inc min-peers)
          allocations
          (reduce
           (fn [all [job k]]
             (assoc all job (if (< k n) max-peers min-peers)))
           {}
           (map vector jobs (range)))])))

(defmethod cjs/job-offer-n-peers :onyx.job-scheduler/balanced
  [{:keys [jobs peers] :as replica}]
  (let [j (count jobs)
        p (count peers)
        min-peers (int (/ p j))
        n (rem p j)
        max-peers (inc min-peers)]
    (reduce
     (fn [all [job k]]
       (assoc all job (if (< k n) max-peers min-peers)))
     {}
     (map vector jobs (range)))))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/balanced
  [replica jobs n]
  ;; First, we separate out the jobs that claimed zero peers because
  ;; they couldn't be covered. We'll merge these jobs back in at the end
  ;; but we're not going to bump their peer levels beyond zero - hence
  ;; the separation. Next, we'll look at the jobs with non-zero claims in
  ;; the order that they were submitted. For each job in turn, we offer it
  ;; one peer from the spare capacity. If it accepts that peer, that job
  ;; goes to the back of the queue to be offered another if there's more.
  ;; If the job's saturation level is met and it can't take on another peer,
  ;; we remove it from the queue. We stop this loop when we either run out of
  ;; jobs or deplete the number of spare peers.
  (let [non-covered-jobs (into {} (filter (fn [[k v]] (zero? v)) jobs))
        covered-jobs (into {} (filter (fn [[k v]] (pos? v)) jobs))
        ordered-jobs (filter (fn [j] (some #{j} (keys covered-jobs))) (:jobs replica))]
    (loop [[head & tail :as job-seq] ordered-jobs
           results covered-jobs
           capacity n]
      (let [tail (vec tail)]
        (cond (or (<= capacity 0) (not (seq job-seq)))
              (merge non-covered-jobs results)
              (< (get results head) (get-in replica [:saturation head]))
              (recur (conj tail head) (update-in results [head] inc) (dec capacity))
              :else
              (recur tail results capacity))))))
