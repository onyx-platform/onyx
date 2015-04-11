(ns onyx.log.commands.gc
  (:require [clojure.set :refer [difference]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :gc
  [{:keys [args message-id]} replica]
  (let [completed (:completed-jobs replica)
        killed (:killed-jobs replica)
        jobs (concat completed killed)]
    (as-> replica x
          (assoc x :killed-jobs [])
          (assoc x :completed-jobs [])
          (reduce (fn [new job] (update-in new [:tasks] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:allocations] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:task-schedulers] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:percentages] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:task-percentages] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:saturation] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:input-tasks] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:output-tasks] dissoc job)) x jobs))))

(defmethod extensions/replica-diff :gc
  [entry old new]
  {:killed-jobs (first (diff (into #{} (:killed-jobs old)) (into #{} (:killed-jobs new))))
   :completed-jobs (first (diff (into #{} (:completed-jobs old)) (into #{} (:completed-jobs new))))
   :tasks (first (diff (:tasks old) (:tasks new)))
   :allocations (first (diff (:allocations old) (:allocations new)))})

(defmethod extensions/reactions :gc
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :gc
  [{:keys [args message-id]} old new diff state]
  (when (= (:id args) (:id state))
    (when (extensions/update-origin! (:log state) new message-id)
      (doseq [k (range 0 message-id)]
        (extensions/gc-log-entry (:log state) k))))
  state)

