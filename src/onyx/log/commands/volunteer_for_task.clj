(ns onyx.log.commands.volunteer-for-task
  (:require [clojure.core.async :refer [chan go >! <! close!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre]))

(defmulti select-task
  (fn [replica job]
    (get-in replica [:task-schedulers job])))

(defmethod select-task :onyx.task-scheduler/greedy
  [replica job]
  (first (get-in replica [:tasks job])))

(defmethod select-task :onyx.task-scheduler/round-robin
  [replica job]
  (let [allocations (get-in replica [:allocations job])
        tasks (get-in replica [:tasks job])]
    (->> tasks
         (map (fn [t] {:task t :n (count (get allocations t))}))
         (sort-by :n)
         (first)
         :task)))

(defmethod select-task :default
  [replica job]
  (throw (ex-info (format "Task scheduler %s not recognized"
                          (get-in replica [:task-schedulers job]))
                  {:replica replica})))

(defn allocations->peers [allocations]
  (reduce-kv
   (fn [all job tasks]
     (merge all
            (reduce-kv
             (fn [all task allocations]
               (->> allocations
                    (map (fn [peer] {peer {:job job :task task}}))
                    (into {})
                    (merge all)))
             {}
             tasks)))
   {}
   allocations))

(defn remove-peers [replica args prev]
  (if (and (:job prev (:task prev)))
    (let [remove-f #(vec (remove (partial = (:id args)) %))]
      (update-in replica [:allocations (:job prev) (:task prev)] remove-f))
    replica))

(defmulti select-job
  (fn [{:keys [args]} replica]
    (:job-scheduler replica)))

(defmethod select-job :onyx.job-scheduler/greedy
  [{:keys [args]} replica]
  (let [job (first (:jobs replica))
        task (select-task replica job)
        prev (get (allocations->peers (:allocations replica)) (:id args))]
    (-> replica
        (update-in [:allocations job task] conj (:id args))
        (update-in [:allocations job task] vec)
        (remove-peers args prev))))

(defn find-job-needing-peers [replica]
  (let [balanced (common/balance-jobs replica)
        counts (common/job->peers replica)]
    (reduce
     (fn [default job]
       (when (< (count (get counts job)) (get balanced job))
         (reduced job)))
     nil
     (:jobs replica))))

(defn round-robin-next-job [replica]
  (let [counts (common/job->peers replica)]
    (ffirst (sort-by count counts))))

(defmethod select-job :onyx.job-scheduler/round-robin
  [{:keys [args]} replica]
  (let [job (or (find-job-needing-peers replica) (round-robin-next-job replica))
        task (select-task replica job)
        prev (get (allocations->peers (:allocations replica)) (:id args))]
    (-> replica
        (update-in [:allocations job task] conj (:id args))
        (update-in [:allocations job task] vec)
        (remove-peers args prev))))

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

