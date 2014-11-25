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

