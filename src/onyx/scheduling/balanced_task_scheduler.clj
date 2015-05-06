(ns onyx.scheduling.balanced-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defn filter-grouped-tasks [replica job allocations]
  (into
   {}
   (remove
    (fn [[k v]]
      (not (nil? (get-in replica [:flux-policies replica job k]))))
    allocations)))

(defmethod cts/drop-peers :onyx.task-scheduler/balanced
  [replica job n]
  (first
   (reduce
    (fn [[peers-to-drop allocations] _]
      (let [max-peers (->> allocations
                           (filter-grouped-tasks replica job)
                           (sort-by (comp count val))
                           reverse
                           first
                           second
                           count)
            task-most-peers (ffirst (filter (fn [x] (= max-peers (count (second x)))) allocations))]
        [(conj peers-to-drop (last (allocations task-most-peers)))
         (update-in allocations [task-most-peers] butlast)]))
    [[] (get-in replica [:allocations job])]
    (range n))))

(defn preallocated-grouped-task? [replica job task]
  (and (not (nil? (get-in replica [:flux-policies job task])))
       (> (count (get-in replica [:allocations job task])) 0)))

(defn reuse-spare-peers [replica job tasks spare-peers]
  (loop [[head & tail :as task-seq] (get-in replica [:tasks job])
         results tasks
         capacity spare-peers]
    (let [tail (vec tail)]
      (cond (or (<= capacity 0) (not (seq task-seq)))
            results
            (and (< (get results head) (or (get-in replica [:task-saturation job head] Double/POSITIVE_INFINITY)))
                 (not (preallocated-grouped-task? replica job head)))
            (recur (conj tail head) (update-in results [head] inc) (dec capacity))
            :else
            (recur tail results capacity)))))

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/balanced
  [replica job n]
  (let [tasks (get-in replica [:tasks job])
        t (apply + (vals (get-in replica [:min-required-peers job])))]
    (if (< n t)
      (zipmap tasks (repeat 0))
      (let [min-peers (int (/ n t))
            r (rem n t)
            max-peers (inc min-peers)
            init
            (reduce
             (fn [all [task k]]
               ;; If it's a grouped task that has already been allocated,
               ;; we can't add more peers since that would break the hashing algorithm.
               (if (preallocated-grouped-task? replica job task)
                 all
                 (assoc all task (min (get-in replica [:task-saturation job task] Double/POSITIVE_INFINITY)
                                      (get-in replica [:min-required-peers job task] Double/POSITIVE_INFINITY)
                                      (if (< k r) max-peers (max min-peers (get-in replica [:min-required-peers job task])))))))
             {}
             (map vector tasks (range)))
            spare-peers (- n (apply + (vals init)))]
        (reuse-spare-peers replica job init spare-peers)))))