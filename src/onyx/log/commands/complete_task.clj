(ns onyx.log.commands.complete-task
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :complete-task
  [{:keys [args message-id]} replica]
  (-> replica
      (update-in [:completions (:job args)] conj (:task args))
      (update-in [:completions (:job args)] vec)
      (update-in [:allocations (:job args)] dissoc (:task args))))

(defmethod extensions/replica-diff :complete-task
  [{:keys [args]} old new]
  {:job (:job args)
   :task (:task args)})

(defmethod extensions/reactions :complete-task
  [{:keys [args]} old new diff peer-args]
  (let [allocations (get-in old [:allocations (:job args) (:task args)])]
    (when (some #{(:id peer-args)} (into #{} allocations))
      [{:fn :volunteer-for-task :args {:id (:id peer-args)}}])))

(defmethod extensions/fire-side-effects! :complete-task
  [{:keys [args]} old new diff state]
  state)

