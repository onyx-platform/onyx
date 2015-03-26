(ns onyx.log.commands.gc
  (:require [clojure.set :refer [difference]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :gc
  [{:keys [args message-id]} replica]
  (let [completed (difference (into #{} (:jobs replica))
                              (into #{} (common/incomplete-jobs replica)))
        killed (:killed-jobs replica)
        jobs (concat completed killed)
        remove-f #(vec (remove (fn [j] (some #{j} jobs)) %))]
    (as-> jobs x
          (reduce (fn [new job] (update-in new [:jobs] remove-f)) replica x)
          (reduce (fn [new job] (update-in new [:killed-jobs] remove-f)) x jobs)
          (reduce (fn [new job] (update-in new [:tasks] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:allocations] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:completions] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:task-schedulers] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:percentages] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:task-percentages] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:saturation] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:input-tasks] dissoc job)) x jobs)
          (reduce (fn [new job] (update-in new [:output-tasks] dissoc job)) x jobs))))

(defmethod extensions/replica-diff :gc
  [entry old new]
  {:jobs (first (diff (into #{} (:jobs old)) (into #{} (:jobs new))))
   :killed-jobs (first (diff (into #{} (:killed-jobs old)) (into #{} (:killed-jobs new))))
   :tasks (first (diff (:tasks old) (:tasks new)))
   :completions (first (diff (:completions old) (:completions new)))
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

