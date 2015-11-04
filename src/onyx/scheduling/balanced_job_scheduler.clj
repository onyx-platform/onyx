(ns onyx.scheduling.balanced-job-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre :refer [info]]
            [onyx.log.commands.common :as common]))

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
  (sort-by (juxt #(common/job-peer-count replica %)
                 #(.indexOf ^clojure.lang.PersistentVector (vec (:jobs replica)) %))
           (:jobs replica)))

;; filter out saturated, then sort by is-covered? (yes before no),
;; then by number of allocated peers

(defn select-job-requiring-peer
  "Selects the next job deserving a peer.
  Tries to cover job requiring the least peers to cover first,
  then tries to balance by peer count"
  [replica jobs]
  (->> jobs
       (sort-by (fn [[job-id peer-count :as job]]
                  (let [covered (max 0 (- (cjs/job-lower-bound replica job-id) peer-count))]
                    (vector covered
                            peer-count
                            (.indexOf ^clojure.lang.PersistentVector (vec (:jobs replica)) job-id)))))
       (remove (fn [[job-id peer-count]]
                 (>= peer-count (cjs/job-upper-bound replica job-id))))
       (ffirst)))

(defmethod cjs/equivalent-allocation? :onyx.job-scheduler/balanced
  [replica replica-new]
  (= (sort (map (fn [[job-id _]]
                  (common/job-peer-count replica job-id))
                (:allocations replica)))
     (sort (map (fn [[job-id _]]
                  (common/job-peer-count replica-new job-id))
                (:allocations replica-new)))))

(defmethod cjs/claim-spare-peers :onyx.job-scheduler/balanced
  [replica jobs n]
  (loop [jobs* jobs n* n]
    (if (zero? n*)
      jobs*
      (let [job (select-job-requiring-peer replica jobs*)]
        (if job
          (recur (update jobs* job inc)
                 (dec n*))
          jobs*)))))
