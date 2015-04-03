(ns onyx.scheduling.balanced-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defn unsaturated-tasks [replica job tasks]
  (filter
   (fn [task]
     (let [allocated (get-in replica [:allocations job task])
           n-allocated (if (seq allocated) (count allocated) 0)]
       (< n-allocated (or (get-in replica [:task-saturation job task]) 
                          Double/POSITIVE_INFINITY))))
   tasks))

(defmethod cts/select-task :onyx.task-scheduler/balanced
  [replica job peer-id]
  (let [allocations (get-in replica [:allocations job])]
    (->> (get-in replica [:tasks job])
         (cts/incomplete-tasks replica job)
         (cts/active-tasks-only replica)
         (unsaturated-tasks replica job)
         (map (fn [t] {:task t :n (count (get allocations t))}))
         (sort-by :n)
         (first)
         :task)))

(defmethod cts/drop-peers :onyx.task-scheduler/balanced
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

(defmethod cts/reallocate-from-task? :onyx.task-scheduler/balanced
  [scheduler old new job state]
  ;;; ??? Cyclic dependency. Needs a rewrite.
  )
