(ns onyx.scheduling.balanced-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.static.util :refer [index-of]]
            [onyx.log.commands.common :as common]))

(defn reuse-spare-peers [replica job tasks spare-peers]
  (loop [task-seq (into #{} (get-in replica [:tasks job]))
         results tasks
         capacity spare-peers]
    (let [least-allocated-task (first (sort-by
                                       (juxt
                                        #(get results %)
                                        #(index-of (get-in replica [:tasks job]) %))
                                       task-seq))]
      (cond
       ;; If there are no more peers to give out, or no more tasks
       ;; want peers, we're done.
       (or (<= capacity 0) (nil? least-allocated-task))
       results

       ;; If we're underneath the saturation level for this task, and this
       ;; task is allowed to be allocated to, we give it one peer and rotate it
       ;; to the back to possibly get more peers later.
       (and (< (get results least-allocated-task)
               (get-in replica [:task-saturation job least-allocated-task] Double/POSITIVE_INFINITY))
            (not (cts/preallocated-grouped-task? replica job least-allocated-task)))
       (recur task-seq (update-in results [least-allocated-task] inc) (dec capacity))

       ;; This task doesn't want more peers, throw it away from the rotating sequence.
       :else
       (recur (disj task-seq least-allocated-task) results capacity)))))

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/balanced
  [replica job n]
  (let [tasks (get-in replica [:tasks job])
        t (cjs/job-lower-bound replica job)]
    (if (< n t)
      (zipmap tasks (repeat 0))
      (let [init
            (reduce
             (fn [all [task k]]
               ;; If it's a grouped task that guarantees consistent hashing,
               ;; we can't add more peers since that would break the hashing algorithm.
               (if (cts/preallocated-grouped-task? replica job task)
                 (assoc all task (count (get-in replica [:allocations job task])))
                 (assoc all task (min (get-in replica [:task-saturation job task] Double/POSITIVE_INFINITY)
                                      (get-in replica [:min-required-peers job task] 1)))))
             {}
             (map vector tasks (range)))
            spare-peers (- n (apply + (vals init)))]
        (reuse-spare-peers replica job init spare-peers)))))
