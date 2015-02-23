(ns onyx.log.commands.common
  (:require [clojure.data :refer [diff]]
            [clojure.set :refer [map-invert]]
            [onyx.extensions :as extensions]))

(defn balance-workload [replica jobs p]
  (if (seq jobs)
    (let [j (count jobs)
          min-peers (int (/ p j))
          n (rem p j)
          max-peers (inc min-peers)]
      (map-indexed
       (fn [i job]
         [job (if (< i n) max-peers min-peers)])
       jobs))
    []))

(defn unbounded-jobs [replica balanced]
  (filter
   (fn [[job _]]
     (= (get-in replica [:saturation job] Double/POSITIVE_INFINITY)
        Double/POSITIVE_INFINITY))
   balanced))

(defn adjust-with-overflow [replica balanced]
  (reduce
   (fn [result [job n]]
     (let [sat (get-in replica [:saturation job] Double/POSITIVE_INFINITY)
           extra (- n sat)]
       (if (pos? extra)
         (-> result
             (update-in [:overflow] + extra)
             (update-in [:jobs] conj [job sat]))
         (-> result
             (update-in [:jobs] conj [job n])))))
   {:overflow 0 :jobs []}
   balanced))

(defn balance-jobs [replica]
  (let [balanced (balance-workload replica (:jobs replica) (count (:peers replica)))
        {:keys [overflow jobs]} (adjust-with-overflow replica balanced)
        unbounded (unbounded-jobs replica jobs)]
    (merge-with
     +
     (into {} (balance-workload replica (map first unbounded) overflow))
     (into {} jobs))))

(defn job->peers [replica]
  (reduce-kv
   (fn [all job tasks]
     (assoc all job (apply concat (vals tasks))))
   {} (:allocations replica)))

(defn peer->allocated-job [allocations id]
  (get
   (reduce-kv
    (fn [all job tasks]
      (->> tasks
           (mapcat (fn [[t ps]] (map (fn [p] {p {:job job :task t}}) ps)))
           (into {})
           (merge all)))
    {} allocations)
   id))

(defn allocations->peers [allocations]
  (reduce-kv
   (fn [all job tasks]
     (merge all
            (reduce-kv
             (fn [all task allocations]
               (->> allocations
                    (map (fn [peer] {peer {:job job :task task}}))
                    (into {})
                    (merge all)))
             {}
             tasks)))
   {}
   allocations))

(defn remove-peers [replica args]
  (let [prev (get (allocations->peers (:allocations replica)) (:id args))]
    (if (and (:job prev) (:task prev))
      (let [remove-f #(vec (remove (partial = (:id args)) %))]
        (update-in replica [:allocations (:job prev) (:task prev)] remove-f))
      replica)))

(defn find-job-needing-peers [replica candidates]
  (let [balanced (balance-jobs replica)
        counts (job->peers replica)]
    (reduce
     (fn [default job]
       (when (< (count (get counts job)) (get balanced job 0))
         (reduced job)))
     nil
     candidates)))

(defn round-robin-next-job [replica candidates]
  (let [counts (job->peers replica)]
    (->> candidates
         (reduce #(conj %1 {:job %2 :n (count (get counts %2))}) [])
         (sort-by :n)
         (first)
         :job)))

(defn saturated-cluster? [replica]
  (let [balanced (balance-jobs replica)
        counts (job->peers replica)]
    (and (= balanced (into {} (map (fn [[job peers]] {job (count peers)}) counts)))
         (= (apply + (vals balanced)) (count (:peers replica))))))

(defn incomplete-jobs [replica]
  (filter
   #(< (count (get-in replica [:completions %]))
       (count (get-in replica [:tasks %])))
   (:jobs replica)))

(defn active-tasks-only
  "Filters out tasks that are currently being sealed."
  [replica tasks]
  (filter #(nil? (get-in replica [:sealing-task %])) tasks))

(defn unsaturated-tasks [replica job tasks]
  (filter
   (fn [task]
     (let [allocated (get-in replica [:allocations job task])
           n-allocated (if (seq allocated) (count allocated) 0)]
       (< n-allocated (or (get-in replica [:task-saturation job task]) Double/POSITIVE_INFINITY))))
   tasks))

(defn jobs-with-available-tasks [replica jobs]
  (filter
   (fn [job]
     (let [tasks (get-in replica [:tasks job])]
       (seq (active-tasks-only replica tasks))))
   jobs))

(defn alive-jobs [replica jobs]
  (let [dead-jobs (into #{} (:killed-jobs replica))]
    (remove (fn [job] (some #{job} dead-jobs)) jobs)))

(defn remove-sealing-tasks [replica args]
  (let [task (get (map-invert (:sealing-tasks replica)) (:id args))]
    (if task
      (update-in replica [:sealing-tasks] dissoc task)
      replica)))

(defn task-status [replica job task]
  (let [peers (get-in replica [:allocations job task])]
    (reduce-kv
     (fn [all k v]
       (assoc all v (+ (get all v 0) 1)))
     {}
     (select-keys (:peer-state replica) peers))))

(defn highest-pct-task [replica job tasks]
  (->> tasks
       (sort-by #(get-in replica [:task-percentages job %]))
       (reverse)
       (first)))

(defn sort-jobs-by-pct [replica]
  (let [indexed
        (map-indexed
         (fn [k j]
           {:position k :job j :pct (get-in replica [:percentages j])})
         (reverse (:jobs replica)))]
    (reverse (sort-by (juxt :pct :position) indexed))))

(defn sort-tasks-by-pct [replica job tasks]
  (let [indexed
        (map-indexed
         (fn [k t]
           {:position k :task t :pct (get-in replica [:task-percentages job t])})
         (reverse tasks))]
    (reverse (sort-by (juxt :pct :position) indexed))))

(defn maximum-jobs-to-use [jobs]
  (reduce
   (fn [all {:keys [pct] :as job}]
     (let [sum (apply + (map :pct all))]
       (if (<= (+ sum pct) 100)
         (conj all job)
         (reduced all))))
   []
   jobs))

(defn min-allocations [jobs n-peers]
  (mapv
   (fn [job]
     (let [n (int (Math/floor (* (* 0.01 (:pct job)) n-peers)))]
       (assoc job :allocation n)))
   jobs))

(defn min-task-allocations [replica job tasks n-peers]
  (mapv
   (fn [task]
     (let [n (int (Math/floor (* (* 0.01 (:pct task)) n-peers)))]
       (assoc task :allocation n)))
   tasks))

(defn percentage-balanced-workload [replica]
  (let [n-peers (count (:peers replica))
        sorted-jobs (sort-jobs-by-pct replica)
        jobs-to-use (maximum-jobs-to-use sorted-jobs)
        init-allocations (min-allocations jobs-to-use n-peers)
        init-usage (apply + (map :allocation init-allocations))
        left-over-peers (- n-peers init-usage)
        with-leftovers (update-in init-allocations [0 :allocation] + left-over-peers)]
    (into {} (map (fn [j] {(:job j) j}) with-leftovers))))

(defn percentage-balanced-taskload [replica job candidate-tasks n-peers]
  (let [sorted-tasks (sort-tasks-by-pct replica job candidate-tasks)
        init-allocations (min-task-allocations replica job sorted-tasks n-peers)
        init-usage (apply + (map :allocation init-allocations))
        left-over-peers (- n-peers init-usage)
        with-leftovers (update-in init-allocations [0 :allocation] + left-over-peers)]
    (into {} (map (fn [t] {(:task t) t}) with-leftovers))))

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

(defmulti drop-peers
  (fn [replica job n]
    (get-in replica [:task-schedulers job])))

(defmethod drop-peers :onyx.task-scheduler/greedy
  [replica job n]
  (let [tasks (get (:allocations replica) job)]
    (take-last n (apply concat (vals tasks)))))

(defmethod drop-peers :onyx.task-scheduler/round-robin
  [replica job n]
  (let [task-seq (cycle (reverse (get-in replica [:tasks job])))]
    (:rets
     (reduce
      (fn [{:keys [rets allocations task-seq] :as vars} _]
        (-> vars
            (update-in [:rets] conj (last (get allocations (first task-seq))))
            (update-in [:allocations (first task-seq)] butlast)
            (update-in [:task-seq] rest)))
      {:rets []
       :allocations (get-in replica [:allocations job])
       :task-seq task-seq}
      (range n)))))

(defmethod drop-peers :onyx.task-scheduler/percentage
  [replica job n]
  (let [tasks (keys (get-in replica [:allocations job]))
        balanced (percentage-balanced-taskload replica job tasks n)]
    (mapcat
     (fn [[task {:keys [allocation]}]]
       (drop-last allocation (get-in replica [:allocations job task])))
     balanced)))

(defmethod drop-peers :default
  [replica job n]
  (let [scheduler (get-in replica [:task-schedulers job])]
    (throw (ex-info 
             (format "Task scheduler %s not recognized. Check that you have not supplied a job scheduler instead." 
                     scheduler)
             {:replica replica}))))

