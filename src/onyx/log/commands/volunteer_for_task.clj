(ns onyx.log.commands.volunteer-for-task
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmulti select-task
  (fn [replica job]
    (get-in replica [:task-schedulers job])))

(defmethod select-task :onyx.task-scheduler/greedy
  [replica job]
  (first (get-in replica [:tasks job])))

(defmethod select-task :onyx.task-scheduler/round-robin
  [replica job]
  (let [task (get-in replica [:last-task-allocated job])
        prev (or task (first (get-in replica [:tasks job])))
        tasks (cycle (get-in replica [:tasks job]))]
    (second (drop-while (partial not= prev) tasks))))

(defmethod select-task :default
  [replica job]
  (throw (ex-info (format "Task scheduler %s not recognized"
                          (get-in replica [:task-schedulers job]))
                  {:replica replica})))

(defmulti select-job
  (fn [{:keys [args]} replica]
    (:job-scheduler replica)))

(defmethod select-job :onyx.job-scheduler/greedy
  [{:keys [args]} replica]
  (let [job (first (:jobs replica))
        task (select-task replica job)]
    (-> replica
        (update-in [:allocations job task] conj (:id args))
        (update-in [:allocations job task] vec)
        (assoc-in [:last-job-allocated] job)
        (assoc-in [:last-task-allocated job] task))))

(defmethod select-job :onyx.job-scheduler/round-robin
  [{:keys [args]} replica]
  (let [prev (or (:last-job-allocated replica) (first (:jobs replica)))
        job (second (drop-while (partial not= prev) (cycle (:jobs replica))))
        task (select-task replica job)]
    (-> replica
        (update-in [:allocations job task] conj (:id args))
        (update-in [:allocations job task] vec)
        (assoc-in [:last-job-allocated] job)
        (assoc-in [:last-task-allocated job] task))))

(defmethod select-job :default
  [_ replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:replica replica})))

(defmethod extensions/apply-log-entry :volunteer-for-task
  [entry replica]
  (select-job entry replica))

(defmethod extensions/replica-diff :volunteer-for-task
  [{:keys [args]} old new]
  {:job (:id args)})

(defmethod extensions/reactions :volunteer-for-task
  [{:keys [args]} old new diff peer-args])

(defmethod extensions/fire-side-effects! :volunteer-for-task
  [entry old new diff state]
  state)

