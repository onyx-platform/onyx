(ns onyx.scheduling.balanced-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.log.commands.common :as common]))

(defmethod cts/drop-peers :onyx.task-scheduler/balanced
  [replica job n]
  (first
   (reduce
    (fn [[peers-to-drop allocations] _]
      (let [max-peers (->> allocations
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

(defmethod cts/task-claim-n-peers :onyx.task-scheduler/balanced
  [replica job n]
  (let [min-required (apply + (vals (get-in replica [:min-required-peers job])))]
    ;; If the number of peers is less than the mininum number of
    ;; required peers, we're not covered - so we claim zero peers.
    ;; Otherwise we take as much as we're allowed, depending on the
    ;; saturation of the job.
    (if (< n min-required)
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
        t (apply + (vals (get-in replica [:min-required-peers job])))]
    (if (< n t)
      (zipmap tasks (repeat 0))
      (let [min-peers (int (/ n t))
            r (rem n t)
            max-peers (inc min-peers)
            init
            (reduce
             (fn [all [task k]]
               (assoc all task (min (get-in replica [:task-saturation job task] Double/POSITIVE_INFINITY)
                                    (get-in replica [:min-required-peers job task] Double/POSITIVE_INFINITY)
                                    (if (< k r) max-peers (max min-peers (get-in replica [:min-required-peers job task]))))))
             {}
             (map vector tasks (range)))
            spare-peers (- n (apply + (vals init)))]
        (reuse-spare-peers replica job init spare-peers)))))