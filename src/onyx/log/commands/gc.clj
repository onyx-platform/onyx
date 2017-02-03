(ns onyx.log.commands.gc
  (:require [clojure.set :refer [difference]]
            [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [warn]]))

(s/defmethod extensions/apply-log-entry :gc :- Replica
  [{:keys [args message-id]} :- LogEntry replica]
  (try
    (let [completed (:completed-jobs replica)
          killed (:killed-jobs replica)
          jobs (into completed killed)]
      (as-> replica x
            (assoc x :killed-jobs [])
            (assoc x :completed-jobs [])
            (reduce (fn [new job] (update-in new [:tasks] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:allocations] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:coordinators] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:task-metadata] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:task-slot-ids] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:flux-policies] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:task-schedulers] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:percentages] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:task-percentages] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:saturation] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:min-required-peers] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:in->out] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:required-tags] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:allocation-version] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:task-saturation] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:input-tasks] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:grouped-tasks] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:state-tasks] dissoc job)) x jobs)
            (reduce (fn [new job] (update-in new [:output-tasks] dissoc job)) x jobs)))
    (catch Throwable e
      (warn e)
      replica)))

(s/defmethod extensions/replica-diff :gc :- ReplicaDiff
  [entry old new]
  {:killed-jobs (first (diff (into #{} (:killed-jobs old)) (into #{} (:killed-jobs new))))
   :completed-jobs (first (diff (into #{} (:completed-jobs old)) (into #{} (:completed-jobs new))))
   :tasks (first (diff (:tasks old) (:tasks new)))
   :allocations (first (diff (:allocations old) (:allocations new)))})

(s/defmethod extensions/reactions [:gc :client] :- Reactions
  [{:keys [args]} :- LogEntry old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! [:gc :client] :- State
  [{:keys [args message-id]} old new diff state]
  (when (= (:id args) (:id state))
    (when (extensions/update-origin! (:log state) new message-id)
      (doseq [k (range 0 message-id)]
        (extensions/gc-log-entry (:log state) k))))
  state)
