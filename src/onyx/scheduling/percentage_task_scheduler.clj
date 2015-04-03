(ns onyx.scheduling.percentage-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :refer [select-task]]))

(defn highest-pct-task [replica job tasks]
  (->> tasks
       (sort-by #(get-in replica [:task-percentages job %]))
       (reverse)
       (first)))

(defn sort-tasks-by-pct [replica job tasks]
  (let [indexed
        (map-indexed
         (fn [k t]
           {:position k :task t :pct (get-in replica [:task-percentages job t])})
         (reverse tasks))]
    (reverse (sort-by (juxt :pct :position) indexed))))

(defn min-task-allocations [replica job tasks n-peers]
  (mapv
   (fn [task]
     (let [n (int (Math/floor (* (* 0.01 (:pct task)) n-peers)))]
       (assoc task :allocation n)))
   tasks))

(defn task-needing-pct-peers [replica job tasks peer]
  (let [allocations (get-in replica [:allocations job])
        total-allocated (count (into #{} (conj (apply concat (vals allocations)) peer)))
        balanced (percentage-balanced-taskload replica job tasks total-allocated)
        sorted-tasks (reverse (sort-by (juxt :pct :position) (vals balanced)))]
    (reduce
     (fn [default t]
       (let [pct (:pct (get balanced (:task t)))
             allocated (get allocations (:task t))
             required (int (Math/floor (* total-allocated (* 0.01 pct))))]
         (if (< (count allocated) required)
           (reduced (:task t))
           default)))
     (:task (first sorted-tasks))
     sorted-tasks)))

(defn percentage-balanced-taskload [replica job candidate-tasks n-peers]
  (let [sorted-tasks (sort-tasks-by-pct replica job candidate-tasks)
        init-allocations (min-task-allocations replica job sorted-tasks n-peers)
        init-usage (apply + (map :allocation init-allocations))
        left-over-peers (- n-peers init-usage)
        with-leftovers (update-in init-allocations [0 :allocation] + left-over-peers)]
    (into {} (map (fn [t] {(:task t) t}) with-leftovers))))

(defmethod select-task :onyx.task-scheduler/percentage
  [replica job peer-id]
  (let [candidates (->> (get-in replica [:tasks job])
                        (incomplete-tasks replica job)
                        (common/active-tasks-only replica))]
    (or (task-needing-pct-peers replica job candidates peer-id)
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

