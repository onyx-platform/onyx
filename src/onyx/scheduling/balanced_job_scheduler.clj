(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

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
