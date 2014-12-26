(ns onyx.log.commands.submit-job
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :submit-job
  [{:keys [args]} replica]
  (-> replica
      (update-in [:jobs] conj (:id args))
      (update-in [:jobs] vec)
      (assoc-in [:task-schedulers (:id args)] (:task-scheduler args))
      (assoc-in [:tasks (:id args)] (vec (:tasks args)))
      (assoc-in [:allocations (:id args)] {})
      (assoc-in [:saturation (:id args)] (:saturation args))))

(defmethod extensions/replica-diff :submit-job
  [{:keys [args]} old new]
  {:job (:id args)})

(defmethod extensions/reactions :submit-job
  [entry old new diff peer-args]
  (cond (and (= (:job-scheduler old) :onyx.job-scheduler/greedy)
             (not (seq (common/alive-jobs old (:jobs old)))))
        [{:fn :volunteer-for-task :args {:id (:id peer-args)}}]
        (= (:job-scheduler old) :onyx.job-scheduler/round-robin)
        (if-let [allocation (common/peer->allocated-job (:allocations new) (:id peer-args))]
          (let [peer-counts (common/balance-jobs new)
                peers (get (common/job->peers new) (:job allocation))]
            (when (> (count peers) (get peer-counts (:job allocation)))
              (let [n (- (count peers) (get peer-counts (:job allocation)))
                    peers-to-drop (common/drop-peers new (:job allocation) n)]
                (when (some #{(:id peer-args)} (into #{} peers-to-drop))
                  [{:fn :volunteer-for-task :args {:id (:id peer-args)}}]))))
          [{:fn :volunteer-for-task :args {:id (:id peer-args)}}])))

(defmethod extensions/fire-side-effects! :submit-job
  [entry old new diff state]
  state)

