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

(defmethod cts/task-claim-n-peers :onyx.task-scheduler/balanced
  [replica job n]
  (let [tasks (get-in replica [:tasks job])]
    ;; If the number of peers is less than the number of tasks,
    ;; we're not covered - so we claim zero peers. Otherwise we
    ;; take as much as we're allowed, depending on the saturation
    ;; of the job.
    (if (< n (count tasks))
      0
      (min (get-in replica [:saturation job] Double/POSITIVE_INFINITY) n))))

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

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/balanced
  [replica job n]
  (let [tasks (get-in replica [:tasks job])
        t (count tasks)
        min-peers (int (/ n t))
        r (rem n t)
        max-peers (inc min-peers)
        init
        (reduce
         (fn [all [task k]]
           (assoc all task (min (get-in replica [:task-saturation job task] Double/POSITIVE_INFINITY)
                                (if (< k r) max-peers min-peers))))
         {}
         (map vector tasks (range)))
        spare-peers (- n (apply + (vals init)))]
    (reuse-spare-peers replica job init spare-peers)))

