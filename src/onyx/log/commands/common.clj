(ns onyx.log.commands.common)

(defn balance-jobs [replica]
  (let [j (count (:jobs replica))
        p (count (:peers replica))
        min-peers (int (/ p j))
        n (rem p j)
        max-peers (inc min-peers)]
    (into {}
          (map-indexed
           (fn [i [job-id tasks]]
             {job-id (if (< i n) max-peers min-peers)})
           (:allocations replica)))))

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

(defn find-job-needing-peers [replica]
  (let [balanced (balance-jobs replica)
        counts (job->peers replica)]
    (reduce
     (fn [default job]
       (when (< (count (get counts job)) (get balanced job))
         (reduced job)))
     nil
     (:jobs replica))))

(defn round-robin-next-job [replica]
  (let [counts (job->peers replica)]
    (ffirst (sort-by count counts))))

(defn saturated-cluster? [replica]
  (let [balanced (balance-jobs replica)
        counts (job->peers replica)]
    (and (= balanced (into {} (map (fn [[job peers]] {job (count peers)}) counts)))
         (= (apply + (vals balanced)) (count (:peers replica))))))

