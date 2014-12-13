(ns onyx.log.commands.complete-task
  (:require [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :complete-task
  [{:keys [args message-id]} replica]
  (let [peers (get-in replica [:allocations (:job args) (:task args)])]
    (-> replica
        (update-in [:completions (:job args)] conj (:task args))
        (update-in [:completions (:job args)] vec)
        (update-in [:allocations (:job args)] dissoc (:task args))
        (merge {:peer-states (into {} (map (fn [p] {p :idle}) peers))}))))

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
  (if (= (:id args) (:id state))
    (do (component/stop (:lifecycle state))
        (assoc state :lifecycle nil))
    state))

