(ns onyx.log.commands.common
  (:require [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]))

(defn balance-workload [replica jobs p]
  (let [j (count jobs)
        min-peers (int (/ p j))
        n (rem p j)
        max-peers (inc min-peers)]
    (map-indexed
     (fn [i job]
       (let [sat (get-in replica [:saturation job] Double/POSITIVE_INFINITY)]
         [job (min sat (if (< i n) max-peers min-peers))]))
     jobs)))

(defn unbounded-jobs [replica balanced]
  (filter
   (fn [[job n]]
     (> (get-in replica [:saturation job] Double/POSITIVE_INFINITY) n))
   balanced))

(defn peer-overflow [replica balanced]
  (reduce
   (fn [sum [job n]]
     (let [sat (or (get-in replica [:saturation job] Double/POSITIVE_INFINITY))
           overflow (- n sat)]
       (if (pos? overflow)
         (+ sum overflow)
         sum)))
   0
   balanced))

(defn balance-jobs [replica]
  (let [balanced (balance-workload replica (:jobs replica) (count (:peers replica)))
        overflow (peer-overflow replica balanced)
        unbounded (unbounded-jobs replica balanced)]
    (merge-with
     (into {} (balance-workload replica (map first unbounded) overflow))
     (into {} balanced)
     (second (diff (into {} unbounded) (into {} balanced))))))

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
    (if (and (:job prev (:task prev)))
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

(defmulti drop-peers
  (fn [replica job n]
    (get-in replica [:task-schedulers job])))

(defmethod drop-peers :onyx.task-scheduler/greedy
  [replica job n]
  (let [tasks (get (:allocations replica) job)]
    (take-last n (apply concat (vals tasks)))))

(defmethod drop-peers :onyx.task-scheduler/round-robin
  [replica job n]
  (let [task-seq (cycle (reverse (get-in replica [:tasks job])))
        rets
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
          (range n)))]
    rets))

(defmethod drop-peers :default
  [replica job n]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:replica replica})))

