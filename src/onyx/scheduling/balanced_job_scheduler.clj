(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

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
  (if (seq jobs)
    (let [j (count jobs)
          p (count peers)
          min-peers (int (/ p j))
          n (rem p j)
          max-peers (inc min-peers)]
      (reduce
       (fn [all [job k]]
         (assoc all job (if (< k n) max-peers min-peers)))
       {}
       (map vector jobs (range))))
    {}))

(defmethod cjs/sort-job-priority :onyx.job-scheduler/balanced
  [replica jobs]
  (sort-by (juxt (fn [job] (apply + (map count (vals (get-in replica [:allocations job])))))
                 #(.indexOf ^clojure.lang.PersistentVector (vec (:jobs replica)) %))
           (:jobs replica)))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/balanced
  [replica jobs n]
  (let [ordered-jobs (sort-by (juxt #(.indexOf ^clojure.lang.PersistentVector (vec (:jobs replica)) %)
                                    #(cjs/job-lower-bound replica %))
                              (:jobs replica))]
        (loop [[head & tail :as job-seq] ordered-jobs
               results jobs
               capacity n]
          (let [tail (vec tail)
                min-peers (cjs/job-lower-bound replica head)
                to-cover (min (- min-peers (get results head 0)) (cjs/job-upper-bound replica head))]
            (cond (or (<= capacity 0) (not (seq job-seq)))
                  results
                  (and (>= capacity to-cover) (pos? to-cover))
                  (recur (conj tail head) (update-in results [head] + to-cover) (- capacity to-cover))
                  (and (< (get results head) (cjs/job-upper-bound replica head)) (pos? (- capacity to-cover)))
                  (recur (conj tail head) (update-in results [head] inc) (dec capacity))
                  :else
                  (recur tail results capacity))))))
