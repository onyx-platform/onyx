(ns onyx.scheduling.percentage-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defn sort-jobs-by-pct [replica jobs]
    (let [indexed
          (map-indexed
           (fn [k j]
             {:position k :job j :pct (get-in replica [:percentages j])})
           (reverse jobs))]
      (reverse (sort-by (juxt :pct :position) indexed))))

(defn min-allocations [jobs n-peers]
  (mapv
   (fn [job]
     (let [n (int (Math/floor (* (* 0.01 (:pct job)) n-peers)))]
       (assoc job :capacity n)))
   jobs))

(defn drop-jobs-overflow [jobs]
  (reduce
   (fn [all {:keys [pct] :as job}]
     (let [sum (apply + (map :pct all))]
       (if (<= (+ sum pct) 100)
         (conj all job)
         (reduced all))))
   []
   jobs))

(defmethod cjs/job-offer-n-peers :onyx.job-scheduler/percentage
  [replica jobs]
  (let [n-peers (count (:peers replica))
        sorted-jobs (sort-jobs-by-pct replica jobs)
        jobs-to-use (drop-jobs-overflow sorted-jobs)
        init-allocations (min-allocations jobs-to-use n-peers)]
    (into {}
          (map
           (fn [j]
             (let [qualified (cjs/n-qualified-peers replica (:peers replica) (:job j))]
               {(:job j) (min qualified (:capacity j))}))
           init-allocations))))

(defn desired-allocation [replica job]
  (* (count (:peers replica))
     0.01
     (get-in replica [:percentages job])))

(defn select-job-requiring-peer
  "Selects the next job deserving a peer.
   Tries to cover job requiring the least peers to cover first,
   then tries to give the peer to whichever job is furthers from its desired
   percentage allocation." 
  [replica jobs]
  (->> jobs
       (sort-by (fn [[job-id peer-count :as job]]
                  (let [covered (max 0 (- (cjs/job-lower-bound replica job-id) peer-count))
                        diff-from-desired (- (common/job-peer-count replica job-id)
                                             (desired-allocation replica job-id))
                        job-index (.indexOf ^clojure.lang.PersistentVector (vec (:jobs replica)) job-id)]
                    (vector covered diff-from-desired job-index))))
       (remove (fn [[job-id peer-count]]
                 (>= peer-count (cjs/job-upper-bound replica job-id))))
       (ffirst)))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/percentage
  [replica jobs n]
  (loop [jobs* jobs n* n]
    (if (zero? n*)
      jobs*
      (let [job (select-job-requiring-peer replica jobs*)]
        (if job
          (recur (update jobs* job inc)
                 (dec n*))
          jobs*)))))
