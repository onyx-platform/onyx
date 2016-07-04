(ns onyx.scheduling.percentage-task-scheduler
  (:require [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.static.util :refer [index-of]]
            [onyx.log.commands.common :as common]))

(defn tasks-by-pct [replica job tasks]
  (sort-by
   (juxt
    :pct
    #(index-of (get-in replica [:tasks job]) (:task %)))
   (map
    (fn [t]
      {:task t :pct (get-in replica [:task-percentages job t])})
    tasks)))

(defn rescale-task-percentages
  "Rescale task percentages after saturated tasks were removed"
  [tasks]
  (let [total (/ (apply + (map :pct tasks)) 100)]
    (map (fn [task]
           (update-in task [:pct] / total))
         tasks)))

(defn remove-grouping-tasks [replica job-id allocations]
  (remove
   (fn [[planned-task allocation]]
     (cts/preallocated-grouped-task? replace job-id planned-task))
   allocations))

(defn reduce-overallocated-peers
  "Turns down the number of peers for tasks where
   we overallocated. This can happen if a grouping task
   is allocated a higher number of peers than its percentage
   value requests. Other tasks must give up peers to compensate.
   Tasks with the highest peer count are prioritized to be
   reduced first."
  [replica job-id planned-allocations]
  (let [sorted-allocations (reverse (sort-by second planned-allocations))
        non-grouping-tasks (remove-grouping-tasks replica job-id sorted-allocations)
        target-task (update-in (first non-grouping-tasks) [1] dec)]
    (merge planned-allocations target-task)))

(defn largest-remainder-allocations
  "Allocates remaining peers to the tasks with the largest remainder.
  e.g. 3 tasks pct allocated 3.5, 1.75, 1.75 -> 3, 2, 2"
  [replica tasks n-peers job]
  (let [tasks* (rescale-task-percentages tasks)
        unrounded (map (fn [task]
                         (cond (cts/preallocated-grouped-task? replica job (:task task))
                               (count (get-in replica [:allocations job (:task task)]))
                               (not (nil? (get-in replica [:flux-policies job (:task task)])))
                               (max (get-in replica [:min-required-peers job (:task task)] Double/POSITIVE_INFINITY)
                                    (* 0.01 (:pct task) n-peers))
                               :else (* 0.01 (:pct task) n-peers)))
                       tasks*)
        full (map int unrounded)
        taken (apply + full)
        remaining (- n-peers taken)
        full-allocated (zipmap tasks* full)
        remainders (->> (map (fn [task v]
                               (vector task (- v (int v))))
                             tasks*
                             unrounded)
                        (sort-by
                         (juxt
                          second
                          #(index-of (reverse (get-in replica [:tasks job])) (:task (first %)))))
                        (reverse)
                        (take remaining)
                        (map (juxt first (constantly 1)))
                        (into {}))
        full-allocated (if (neg? remaining)
                         (reduce-overallocated-peers replica job full-allocated)
                         full-allocated)
        final-allocations (merge-with + full-allocated remainders)]
    (mapv (fn [[task allocation]]
            (assoc task :allocation (int allocation)))
          final-allocations)))

(defn percentage-balanced-taskload
  [replica job candidate-tasks n-peers]
  {:post [(>= n-peers 0)]}
  (let [sorted-tasks (tasks-by-pct replica job candidate-tasks)
        allocations (largest-remainder-allocations replica sorted-tasks n-peers job)
        oversaturated (filter (fn [{:keys [task allocation]}]
                                (> allocation (get-in replica [:task-saturation job task])))
                              allocations)
        cutoff-oversaturated (->> oversaturated
                                  (map (fn [{:keys [task] :as t}]
                                         [task (assoc t :allocation (get-in replica [:task-saturation job task]))]))
                                  (into {}))]
    (if (empty? cutoff-oversaturated)
      (into {} (map (fn [t] {(:task t) t}) allocations))
      (let [n-peers-fully-saturated (apply + (map :allocation (vals cutoff-oversaturated)))
            n-remaining-peers (- n-peers n-peers-fully-saturated)
            unallocated-tasks (remove cutoff-oversaturated candidate-tasks)]
        (merge (percentage-balanced-taskload replica job unallocated-tasks n-remaining-peers)
               cutoff-oversaturated)))))

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/percentage
  [replica job n]
  (let [tasks (get-in replica [:tasks job])
        t (cjs/job-lower-bound replica job)]
    (if (< n t)
      (zipmap tasks (repeat 0))
      (let [grouped (filter (partial cts/preallocated-grouped-task? replica job) tasks)
            not-grouped (remove (partial cts/preallocated-grouped-task? replica job) tasks)
            init (into {} (map (fn [t] {t (count (get-in replica [:allocations job t]))}) grouped))
            spare-peers (- n (apply + (vals init)))
            balanced (percentage-balanced-taskload replica job not-grouped spare-peers)
            rets (into {} (map (fn [[k v]] {k (:allocation v)}) balanced))]
        (merge init rets)))))
