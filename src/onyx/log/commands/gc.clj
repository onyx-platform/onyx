(ns onyx.log.commands.gc
  (:require [clojure.set :refer [difference]]
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
          (reduce (fn [new job] (update-in new [:task-percentages] dissoc job)) x jobs))))

(defmethod extensions/replica-diff :gc
  [entry old new])

(defmethod extensions/fire-side-effects! :gc
  [{:keys [args]} old new diff state]
  state)

(defmethod extensions/reactions :gc
  [{:keys [args]} old new diff peer-args])

