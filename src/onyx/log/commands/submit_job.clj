(ns onyx.log.commands.submit-job
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :submit-job
  [{:keys [args]} replica]
  (-> replica
      (update-in [:jobs] conj (:id args))
      (update-in [:jobs] vec)
      (assoc-in [:task-schedulers (:id args)] (:task-scheduler args))
      (assoc-in [:tasks (:id args)] (:tasks args))
      (assoc-in [:allocations (:id args)] {})))

(defmethod extensions/replica-diff :submit-job
  [{:keys [args]} old new]
  {:job (:id args)})

(defmulti drop-peers
  (fn [replica job n]
    (get-in replica [:task-schedulers job])))

(defmethod drop-peers :onyx.task-scheduler/greedy
  [replica job n]
  (let [tasks (get (:allocations replica) job)]
    (take-last n (apply concat (vals tasks)))))

(defmethod drop-peers :onyx.task-scheduler/round-robin
  [replica job n])

(defmethod drop-peers :default
  [replica job n]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:replica replica})))

(defn balance-jobs [replica]
  (let [j (count (:jobs replica))
        p (count (:peers replica))
        min-peers (int (/ p j))
        r (/ min-peers j)
        n (if (integer? r) 0 (numerator r))
        max-peers (inc min-peers)]
    (into {}
          (map-indexed
           (fn [i [job-id tasks]]
             {job-id (if (<= i n) max-peers min-peers)})
           (:allocations replica)))))

(defn job->peers [replica]
  (reduce-kv
   (fn [all job tasks]
     (assoc all job (apply concat (vals tasks))))
   {} (:allocations replica)))

(defn peer->allocated-job [allocations id]
  (get
   (reduce-kv
    (fn [all job tasks]
      (->> tasks
           (mapcat (fn [[t ps]] (map (fn [p] {p {:job job :task t}}) ps)))
           (into {})
           (merge all)))
    {} allocations)
   id))

(defmethod extensions/reactions :submit-job
  [entry old new diff peer-args]
  (cond (and (= (:job-scheduler old) :onyx.job-scheduler/greedy)
             (not (seq (:jobs old))))
        [{:fn :volunteer-for-task :args {:id (:id peer-args)}}]
        (= (:job-scheduler old) :onyx.job-scheduler/round-robin)
        (if-let [allocation (peer->allocated-job (:allocations new) (:id peer-args))]
          (let [peer-counts (balance-jobs new)
                peers (get (job->peers new) (:job allocation))]
            (when (> (count peers) (get peer-counts (:job allocation)))
              (let [n (- (count peers) (get peer-counts (:job allocation)))
                    peers-to-drop (drop-peers new (:job allocation) n)]
                (when (some #{(:id peer-args)} (into #{} peers-to-drop))
                  [{:fn :volunteer-for-task :args {:id (:id peer-args)}}]))))
          [{:fn :volunteer-for-task :args {:id (:id peer-args)}}])))

(defmethod extensions/fire-side-effects! :submit-job
  [entry old new diff state]
  state)

