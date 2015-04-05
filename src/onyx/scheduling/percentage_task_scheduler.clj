(ns onyx.scheduling.percentage-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

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

(defn percentage-balanced-taskload [replica job candidate-tasks n-peers]
  (let [sorted-tasks (sort-tasks-by-pct replica job candidate-tasks)
        init-allocations (min-task-allocations replica job sorted-tasks n-peers)
        init-usage (apply + (map :allocation init-allocations))
        left-over-peers (- n-peers init-usage)
        with-leftovers (update-in init-allocations [0 :allocation] + left-over-peers)]
    (into {} (map (fn [t] {(:task t) t}) with-leftovers))))

(defmethod cts/drop-peers :onyx.task-scheduler/percentage
  [replica job n]
  (let [tasks (keys (get-in replica [:allocations job]))
        balanced (percentage-balanced-taskload replica job tasks n)]
    (mapcat
     (fn [[task {:keys [allocation]}]]
       (drop-last allocation (get-in replica [:allocations job task])))
     balanced)))

(defmethod cts/task-claim-n-peers :onyx.task-scheduler/percentage
  [replica job n]
  ;; We can reuse the Balanced task scheduler algorithm as is.
  (cts/task-claim-n-peers
   (assoc-in replica [:task-schedulers job] :onyx.task-scheduler/balanced)
   job n))

(defn reuse-spare-peers [replica job tasks spare-peers]
  (loop [[head & tail :as task-seq] (get-in replica [:tasks job])
         results tasks
         capacity spare-peers]
    (let [tail (vec tail)]
      (cond (or (<= capacity 0) (not (seq task-seq)))
            results
            (< (get results head) (or (get-in replica [:task-saturation job head] Double/POSITIVE_INFINITY)))
            (recur (conj tail head) (update-in results [head] inc) (dec capacity))
            :else
            (recur tail results capacity)))))

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/percentage
  [replica job n]
  (let [tasks (get-in replica [:tasks job])
        init
        (reduce
         (fn [all [task k]]
           (assoc all task (min (get-in replica [:task-saturation job task] Double/POSITIVE_INFINITY)
                                (int (* n (get-in replica [:task-percentages job task]) 0.01)))))
         {}
         (map vector tasks (range)))
        spare-peers (- n (apply + (vals init)))]
    (reuse-spare-peers replica job init spare-peers)))
