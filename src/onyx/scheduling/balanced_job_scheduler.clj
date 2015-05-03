(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defn job-coverable? [replica job n]
  (>= n (apply + (vals (get-in replica [:min-required-peers job])))))

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

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/balanced
  [replica jobs n]
  (let [ordered-jobs (sort-by (juxt #(.indexOf (:jobs replica) %)
                                    #(apply + (vals (get-in replica [:min-required-peers %]))))
                              (:jobs replica))]
    (loop [[head & tail :as job-seq] ordered-jobs
           results jobs
           capacity n]
      (let [tail (vec tail)
            min-peers (apply + (vals (get-in replica [:min-required-peers head])))
            to-cover (- min-peers (get results head 0))]
        (cond (or (<= capacity 0) (not (seq job-seq)))
              results
              (and (>= capacity to-cover) (pos? to-cover))
              (recur (conj tail head) (update-in results [head] + to-cover) (- capacity to-cover))
              (and (< (get results head) (get-in replica [:saturation head])) (pos? (- capacity to-cover)))
              (recur (conj tail head) (update-in results [head] inc) (dec capacity))
              :else
              (recur tail results capacity))))))
