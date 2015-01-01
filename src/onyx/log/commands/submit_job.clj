(ns onyx.log.commands.submit-job
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmulti scheduler-replica-update
  (fn [replica entry]
    (:job-scheduler replica)))

(defmethod scheduler-replica-update :onyx.job-scheduler/percentage
  [replica {:keys [args]}]
  (assoc-in replica [:percentages (:id args)] (:percentage args)))

(defmethod scheduler-replica-update :default
  [replica entry]
  replica)

(defmethod extensions/apply-log-entry :submit-job
  [{:keys [args] :as entry} replica]
  (-> replica
      (update-in [:jobs] conj (:id args))
      (update-in [:jobs] vec)
      (assoc-in [:task-schedulers (:id args)] (:task-scheduler args))
      (assoc-in [:tasks (:id args)] (vec (:tasks args)))
      (assoc-in [:allocations (:id args)] {})
      (assoc-in [:saturation (:id args)] (:saturation args))
      (scheduler-replica-update entry)))

(defmethod extensions/replica-diff :submit-job
  [{:keys [args]} old new]
  {:job (:id args)})

(defmulti reallocate?
  (fn [scheduler old new state]
    scheduler))

(defmethod reallocate? :onyx.job-scheduler/greedy
  [scheduler old new state]
  (not (seq (common/alive-jobs old (:jobs old)))))

(defmethod reallocate? :onyx.job-scheduler/round-robin
  [scheduler old new state]
  (if-let [allocation (common/peer->allocated-job (:allocations new) (:id state))]
    (let [peer-counts (common/balance-jobs new)
          peers (get (common/job->peers new) (:job allocation))]
      (when (> (count peers) (get peer-counts (:job allocation)))
        (let [n (- (count peers) (get peer-counts (:job allocation)))
              peers-to-drop (common/drop-peers new (:job allocation) n)]
          (when (some #{(:id state)} (into #{} peers-to-drop))
            true))))
    true))

(defn sort-jobs-by-pct [replica]
  (let [indexed
        (map-indexed
         (fn [k j]
           {:position k :job j :pct (get-in replica [:percentages j])})
         (reverse (:jobs replica)))]
    (reverse (sort-by (juxt :pct :position) indexed))))

(defn maximum-jobs-to-use [jobs]
  (reduce
   (fn [all {:keys [pct] :as job}]
     (let [sum (apply + (map :pct all))]
       (if (<= (+ sum pct) 100)
         (conj all job)
         (reduced all))))
   []
   jobs))

(defn min-allocations [jobs n-peers]
  (mapv
   (fn [job]
     (let [n (int (Math/floor (* (* 0.01 (:pct job)) n-peers)))]
       (assoc job :allocation n)))
   jobs))

(defn percentage-balanced-workload [replica]
  (let [n-peers (count (:peers replica))
        sorted-jobs (sort-jobs-by-pct replica)
        jobs-to-use (maximum-jobs-to-use sorted-jobs)
        init-allocations (min-allocations jobs-to-use n-peers)
        init-usage (apply + (map :allocation init-allocations))
        left-over-peers (- n-peers init-usage)]
    (update-in init-allocations [0 :allocation] + left-over-peers)))

(defmethod reallocate? :onyx.job-scheduler/percentage
  [scheduler old new state]
  (if-let [allocation (common/peer->allocated-job (:allocations new) (:id state))]
    (let [balanced (percentage-balanced-workload new)
          peer-counts (first (filter #(= (:job %) (:job allocation)) balanced))
          peers (get (common/job->peers new) (:job allocation))]
      (when (> (count peers) (get peer-counts (:job allocation)))
        (let [n (- (count peers) (get peer-counts (:job allocation)))
              peers-to-drop (common/drop-peers new (:job allocation) n)]
          (when (some #{(:id state)} (into #{} peers-to-drop))
            true))))
    true))

(defmethod extensions/reactions :submit-job
  [entry old new diff peer-args]
  (when (reallocate? (:job-scheduler old) old new peer-args)
    [{:fn :volunteer-for-task :args {:id (:id peer-args)}}]))

(defmethod extensions/fire-side-effects! :submit-job
  [entry old new diff state]
  state)

