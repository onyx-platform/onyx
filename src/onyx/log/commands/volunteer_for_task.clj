(ns onyx.log.commands.volunteer-for-task
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
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

(defmethod select-task :onyx.task-scheduler/round-robin
  [replica job peer-id]
  (let [allocations (get-in replica [:allocations job])]
    (->> (get-in replica [:tasks job])
         (incomplete-tasks replica job)
         (common/active-tasks-only replica)
         (common/unsaturated-tasks replica job)
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
  (throw (ex-info 
           (format "Task scheduler %s not recognized. Check that you have not supplied a job scheduler instead."
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

(defn exempt-from-acker? [replica job task args]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn offer-acker [replica job task args]
  (let [peers (count (apply concat (vals (get-in replica [:allocations job]))))
        ackers (count (get-in replica [:ackers job]))
        pct (get-in replica [:acker-percentage job])
        current-pct (int (Math/ceil (* 10 (double (/ ackers peers)))))]
    (if (and (< current-pct pct) (not (exempt-from-acker? replica job task args)))
      (-> replica
          (update-in [:ackers job] conj (:id args))
          (update-in [:ackers job] vec))
      replica)))

(defmethod select-job :onyx.job-scheduler/greedy
  [{:keys [args]} replica]
  (let [job (first (universally-executable-jobs replica))
        allocation (common/peer->allocated-job (:allocations replica) (:id args))]
    (if job
      (if-let [task (select-task replica job (:id args))]
        (if (or (not= task (:task allocation))
                (not= job (:job allocation)))
          (-> replica
              (common/remove-peers args)
              (update-in [:allocations job task] conj (:id args))
              (update-in [:allocations job task] vec)
              (assoc-in [:peer-state (:id args)] :warming-up)
              (offer-acker job task args))
          replica)
        replica)
      replica)))

(defmethod select-job :onyx.job-scheduler/round-robin
  [{:keys [args]} replica]
  (if-not (common/saturated-cluster? replica)
    (let [candidates (universally-executable-jobs replica)
          allocation (common/peer->allocated-job (:allocations replica) (:id args))
          job (or (common/find-job-needing-peers replica candidates)
                  (common/round-robin-next-job replica candidates))]
      (if job
        (if-let [task (select-task replica job (:id args))]
          (if (or (not= task (:task allocation))
                  (not= job (:job allocation)))
            (-> replica
                (common/remove-peers args)
                (update-in [:allocations job task] conj (:id args))
                (update-in [:allocations job task] vec)
                (assoc-in [:peer-state (:id args)] :warming-up)
                (offer-acker job task args))
            replica)
          replica)
        replica))
    replica))

(defmethod select-job :onyx.job-scheduler/percentage
  [{:keys [args]} replica]
  (let [candidates (universally-executable-jobs replica)
        balanced (common/percentage-balanced-workload replica)
        allocation (common/peer->allocated-job (:allocations replica) (:id args))
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
      (if-let [task (select-task replica job (:id args))]
        (if (or (not= task (:task allocation))
                (not= job (:job allocation)))
          (-> replica
              (common/remove-peers args)
              (update-in [:allocations job task] conj (:id args))
              (update-in [:allocations job task] vec)
              (assoc-in [:peer-state (:id args)] :warming-up)
              (offer-acker job task args))
          replica)
        replica)
      replica)))

(defmethod select-job :default
  [_ replica]
  (throw (ex-info 
           (format "Job scheduler %s not recognized. Check that you have not supplied a task scheduler instead." 
                   (:job-scheduler replica))
           {:replica replica})))

(defmethod extensions/apply-log-entry :volunteer-for-task
  [entry replica]
  (select-job entry replica))

(defmethod extensions/replica-diff :volunteer-for-task
  [{:keys [args]} old new]
  (let [allocation (second (diff (:allocations old) (:allocations new)))]
    {:job (first (keys allocation))
     :task (first (keys (get allocation (first (keys allocation)))))}))

(defmethod extensions/reactions :volunteer-for-task
  [{:keys [args]} old new diff peer-args]
  (let [scheduler (get-in new [:task-schedulers (:job diff)])]
    (when (and (common/volunteer? old new peer-args (:job peer-args))
               (common/reallocate-from-task? scheduler old new (:job diff) peer-args))
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
              new-lifecycle (future (component/start ((:task-lifecycle-fn state) diff new-state)))]
          (assoc new-state :lifecycle new-lifecycle :seal-response-ch seal-ch)))
    state))

