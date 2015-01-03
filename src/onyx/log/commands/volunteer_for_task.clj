(ns onyx.log.commands.volunteer-for-task
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]
            [onyx.extensions :as extensions]
            [taoensso.timbre]))

(defmulti select-task
  (fn [replica job peer-id]
    (get-in replica [:task-schedulers job])))

(defn incomplete-tasks [replica job tasks]
  (let [tasks (get-in replica [:tasks job])
        completed (get-in replica [:completions job])]
    (filter identity (second (diff completed tasks)))))

(defmethod select-task :onyx.task-scheduler/greedy
  [replica job peer-id]
  (->> (get-in replica [:tasks job])
       (incomplete-tasks replica job)
       (common/active-tasks-only replica)
       (first)))

(defmethod select-task :onyx.task-scheduler/round-robin
  [replica job peer-id]
  (let [allocations (get-in replica [:allocations job])]
    (->> (get-in replica [:tasks job])
         (incomplete-tasks replica job)
         (common/active-tasks-only replica)
         (map (fn [t] {:task t :n (count (get allocations t))}))
         (sort-by :n)
         (first)
         :task)))

(defmethod select-task :onyx.task-scheduler/percentage
  [replica job peer-id]
  (let [candidates (->> (get-in replica [:tasks job])
                        (incomplete-tasks replica job)
                        (common/active-tasks-only replica))]
    (or (common/task-needing-pct-peers replica job candidates peer-id)
        (common/highest-pct-task replica job candidates))))

(defmethod select-task :default
  [replica job peer-id]
  (throw (ex-info (format "Task scheduler %s not recognized"
                          (get-in replica [:task-schedulers job]))
                  {:replica replica})))

(defmulti select-job
  (fn [{:keys [args]} replica]
    (:job-scheduler replica)))

(defn universally-executable-jobs [replica]
  (->> replica
       (common/incomplete-jobs)
       (common/alive-jobs replica)
       (common/jobs-with-available-tasks replica)))

(defmethod select-job :onyx.job-scheduler/greedy
  [{:keys [args]} replica]
  (let [job (first (universally-executable-jobs replica))]
    (if job
      (let [task (select-task replica job (:id args))]
        (-> replica
            (common/remove-peers args)
            (update-in [:allocations job task] conj (:id args))
            (update-in [:allocations job task] vec)
            (assoc-in [:peer-state (:id args)] :active)))
      replica)))

(defmethod select-job :onyx.job-scheduler/round-robin
  [{:keys [args]} replica]
  (if-not (common/saturated-cluster? replica)
    (let [candidates (universally-executable-jobs replica)
          job (or (common/find-job-needing-peers replica candidates)
                  (common/round-robin-next-job replica candidates))]
      (if job
        (let [task (select-task replica job (:id args))]
          (-> replica
              (common/remove-peers args)
              (update-in [:allocations job task] conj (:id args))
              (update-in [:allocations job task] vec)
              (assoc-in [:peer-state (:id args)] :active)))
        replica))
    replica))

(defmethod select-job :onyx.job-scheduler/percentage
  [{:keys [args]} replica]
  (let [candidates (universally-executable-jobs replica)
        balanced (common/percentage-balanced-workload replica)
        job
        (reduce
         (fn [_ job]
           (let [required-count (:allocation (get balanced job))
                 actual-count (count (get (common/job->peers replica) job))]
             (when (< actual-count required-count)
               (reduced job))))
         nil
         candidates)]
    (if job
      (let [task (select-task replica job (:id args))]
        (-> replica
            (common/remove-peers args)
            (update-in [:allocations job task] conj (:id args))
            (update-in [:allocations job task] vec)
            (assoc-in [:peer-state (:id args)] :active)))
      replica)))

(defmethod select-job :default
  [_ replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:replica replica})))

(defmethod extensions/apply-log-entry :volunteer-for-task
  [entry replica]
  (select-job entry replica))

(defmethod extensions/replica-diff :volunteer-for-task
  [{:keys [args]} old new]
  (let [allocation (second (diff (:allocations old) (:allocations new)))]
    {:job (first (keys allocation))
     :task (first (keys (get allocation (first (keys allocation)))))}))

(defmulti reallocate-from-task?
  (fn [scheduler old new job state]
    scheduler))

(defmethod reallocate-from-task? :onyx.task-scheduler/percentage
  [scheduler old new job state]
  (let [allocation (common/peer->allocated-job (:allocations new) (:id state))]
    (when (= (:job allocation) job)
      (let [candidate-tasks (keys (get-in new [:allocations job]))
            n-peers (count (apply concat (vals (get-in new [:allocations job]))))
            balanced (common/percentage-balanced-taskload new job candidate-tasks n-peers)
            required (:allocation (get balanced (:task allocation)))
            actual (count (get-in new [:allocations job (:task allocation)]))]
        (when (and (> actual required))
          (let [n (- actual required)
                peers-to-drop (common/drop-peers new job n)]
            (when (some #{(:id state)} (into #{} peers-to-drop))
              true)))))))

(defmethod reallocate-from-task? :default
  [scheduler old new job state]
  false)

(defmethod extensions/reactions :volunteer-for-task
  [{:keys [args]} old new diff peer-args]
  (let [scheduler (get-in new [:task-schedulers (:job diff)])]
    (when (reallocate-from-task? scheduler old new (:job diff) peer-args)
      [{:fn :volunteer-for-task :args {:id (:id peer-args)}}])))

(defmethod extensions/fire-side-effects! :volunteer-for-task
  [{:keys [args]} old new diff state]
  (if (and (= (:id args) (:id state)) (:job diff) (:task diff)
           (or (not= (:job state) (:job diff))
               (not= (:task state) (:task diff))))
    (do (when (:lifecycle state)
          (component/stop (:lifecycle state)))
        (let [seal-ch (chan)
              new-state (assoc state :job (:job diff) :task (:task diff) :seal-ch seal-ch)
              new-lifecycle (component/start ((:task-lifecycle-fn state) diff new-state))]
          (assoc new-state :lifecycle new-lifecycle :seal-response-ch seal-ch)))
    state))

