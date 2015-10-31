(ns onyx.scheduling.percentage-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

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

(defn job-peer-count [replica job]
  (apply + (map count (vals (get-in replica [:allocations job])))))

(defmethod cjs/sort-job-priority :onyx.job-scheduler/percentage
  [replica jobs]
  (sort-by (juxt #(.indexOf ^clojure.lang.PersistentVector (vec (:jobs replica)) %)
                 (partial job-peer-count replica))
           (:jobs replica)))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/percentage
  [replica jobs n]
  ;; We can get away with using the exact same algorithm as the
  ;; Balanced job scheduler.
  (cjs/claim-spare-peers
   (assoc replica :job-scheduler :onyx.job-scheduler/balanced) jobs n))
