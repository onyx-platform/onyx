(ns onyx.scheduling.percentage-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :refer [select-task]]))

(defmethod select-task :onyx.task-scheduler/percentage
  [replica job peer-id]
  (let [candidates (->> (get-in replica [:tasks job])
                        (incomplete-tasks replica job)
                        (common/active-tasks-only replica))]
    (or (common/task-needing-pct-peers replica job candidates peer-id)
        (common/highest-pct-task replica job candidates))))

(defmethod drop-peers :onyx.task-scheduler/percentage
  [replica job n]
  (let [tasks (keys (get-in replica [:allocations job]))
        balanced (percentage-balanced-taskload replica job tasks n)]
    (mapcat
     (fn [[task {:keys [allocation]}]]
       (drop-last allocation (get-in replica [:allocations job task])))
     balanced)))

(defmethod reallocate-from-task? :onyx.task-scheduler/percentage
  [scheduler old new job state]
  (let [allocation (peer->allocated-job (:allocations new) (:id state))]
    (when (= (:job allocation) job)
      (let [candidate-tasks (keys (get-in new [:allocations job]))
            n-peers (count (apply concat (vals (get-in new [:allocations job]))))
            balanced (percentage-balanced-taskload new job candidate-tasks n-peers)
            required (:allocation (get balanced (:task allocation)))
            actual (count (get-in new [:allocations (:job allocation) (:task allocation)]))]
        (when (> actual required)
          (let [n (- actual required)
                peers-to-drop (drop-peers new (:job allocation) n)]
            (when (some #{(:id state)} (into #{} peers-to-drop))
              true)))))))

