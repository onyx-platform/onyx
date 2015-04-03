(ns onyx.scheduling.balanced-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :refer [select-task]]))

(defmethod select-task :onyx.task-scheduler/balanced
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

(defmethod drop-peers :onyx.task-scheduler/balanced
  [replica job n]
  (first
    (reduce
      (fn [[peers-to-drop allocations] _]
        (let [task-most-peers (->> allocations 
                                   (sort-by (comp count val))
                                   reverse
                                   ffirst)] 
          [(conj peers-to-drop (last (allocations task-most-peers)))
           (update-in allocations [task-most-peers] butlast)]))
      [[] (get-in replica [:allocations job])] 
      (range n))))

(defmethod reallocate-from-task? :onyx.task-scheduler/balanced
  [scheduler old new job state]
  (let [allocations (balance-jobs new)
        allocation (peer->allocated-job (:allocations new) (:id state))
        required (get allocations job)
        actual (count (apply concat (vals (get-in old [:allocations (:job allocation)]))))]
    (when (> actual required)
      (let [peers-to-drop (vector (first (drop-peers new (:job allocation) (- actual required))))]
        (some #{(:id state)} peers-to-drop)))))
