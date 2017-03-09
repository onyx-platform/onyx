(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre :refer [info]]
            [onyx.static.util :refer [index-of]]
            [onyx.log.commands.common :as common]))

(defmethod cjs/job-offer-n-peers :onyx.job-scheduler/balanced
  [{:keys [peers] :as replica} jobs]
  (if (seq jobs)
    (let [j (count jobs)
          p (count peers)
          min-peers (int (/ p j))
          n (rem p j)
          max-peers (inc min-peers)]
      (reduce
       (fn [all [job k]]
         (let [qualified-peers (cjs/n-qualified-peers replica peers job)]
           (assoc all job
                  (if (< k n)
                    (min qualified-peers max-peers)
                    (min qualified-peers min-peers)))))
       {}
       (map vector jobs (range))))
    {}))

;; filter out saturated, then sort by is-covered? (yes before no),
;; then by number of allocated peers
(defn select-job-requiring-peer
  "Selects the next job deserving a peer.
  Tries to cover job requiring the least peers to cover first,
  then tries to balance by peer count"
  [replica jobs n-remaining]
  (->> jobs
       ;; remove all fully allocated jobs
       (remove (fn [[job-id peer-count]]
                 (>= peer-count (cjs/job-upper-bound replica job-id))))
       ;; only allocate to jobs that are potentially coverable or already covered
       (filter (fn [[job-id peer-count]]
                 (>= (+ peer-count n-remaining) 
                     (cjs/job-lower-bound replica job-id))))
       (sort-by (fn [[job-id peer-count :as job]]
                  (let [covered? (>= peer-count (cjs/job-lower-bound replica job-id))
                        remaining-required (max 0 (- (cjs/job-lower-bound replica job-id) peer-count))]
                    (vector ;; try to assign to uncovered over covered
                            covered?
                            ;; then assign to the job with the least remaining required
                            remaining-required
                            ;; uncovered, lowest peer count
                            peer-count
                            (index-of (:jobs replica) job-id)))))
       (ffirst)))


;; Take all the claimable peers
;; 1. Allocate as many to the first uncovered job that you can fully cover with all those peers
;;    up to however many it requires
;; 2. If there are no jobs that can be allocated, allocate one peer to the job with 
;;    the minimum number of peers that is not fully covered
;; TODO: this should be replaced by btrplace constraints
(defmethod cjs/claim-spare-peers :onyx.job-scheduler/balanced
  [replica jobs n]
  (loop [jobs* jobs n* n]
    (if (zero? n*)
      jobs*
      (let [job (select-job-requiring-peer replica jobs* n*)]
        (if job
          (recur (update jobs* job inc)
                 (dec n*))
          jobs*)))))
