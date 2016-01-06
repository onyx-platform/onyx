(ns onyx.scheduling.colocated-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.extensions :as extensions])
  (:import [org.btrplace.model.constraint SplitAmong Ban]))

(defn site->peers [replica]
  (group-by
   (fn [p] (extensions/get-peer-site replica p))
   (:peers replica)))

(defn large-enough-sites [site->peers-mapping min-peers]
  (reduce-kv
   (fn [result peer-site peers]
     (if (>= (count peers) min-peers)
       (assoc result peer-site peers)
       result))
   {}
   site->peers-mapping))

(defn global-saturation-lower-bound [replica job-id task-ids]
  (let [saturation-values
        (->> task-ids
             (map #(get-in replica [:task-saturation job-id %]))
             (remove nil?))]
    (if (seq saturation-values)
      (apply min saturation-values)
      Double/POSITIVE_INFINITY)))

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/colocated
  [replica job-id n]
  (let [task-ids (get-in replica [:tasks job-id])
        capacity (count task-ids)
        site->peers-mapping (large-enough-sites (site->peers replica) capacity)
        n-candidate-peers (apply + (map count (vals site->peers-mapping)))
        lower-bound (global-saturation-lower-bound replica job-id task-ids)
        upper-bound (int (/ n-candidate-peers capacity))]
    (zipmap task-ids (repeat (min lower-bound upper-bound)))))

(defn collect-nodes-for-jobs [replica jobs task->node]
  (mapcat
   (fn [job-id]
     (map
      (fn [task-id] (task->node [job-id task-id]))
      (get-in replica [:tasks job-id])))
   jobs))

(defn ban-smaller-sites [replica jobs peer->vm task->node large-sites rejected]
  (let [sites (keys large-sites)
        peer-ids (into
                  ((group-by
                    #(some #{(extensions/get-peer-site replica %)} sites)
                    (:peers replica))
                   nil)
                  rejected)
        jobs (filter #(= (get-in replica [:task-schedulers %])
                         :onyx.task-scheduler/colocated)
                     jobs)
        nodes (collect-nodes-for-jobs replica jobs task->node)]
    (map #(Ban. (peer->vm %) nodes) peer-ids)))

(defn non-colocated-tasks [replica jobs]
  (reduce
   (fn [result job-id]
     (if (not= (get-in replica [:task-schedulers job-id])
               :onyx.task-scheduler/colocated)
       (into result (map #(-> [job-id %]) (get-in replica [:tasks job-id])))
       result))
   []
   jobs))

(defn select-peers [site->peers total size]
  (assert (zero? (mod total size)))
  (reduce-kv
   (fn [{:keys [selected rejected] :as result} site peer-ids]
     (if (>= (count selected) total)
       (update-in result [:rejected] into peer-ids)
       (let [difference (- total (count selected))
             remainder (mod (count peer-ids) size)
             selected (take difference (drop-last remainder peer-ids))
             rejects (take-last (mod (count peer-ids) size) peer-ids)]
         (-> result
             (update-in [:selected] into selected)
             (update-in [:rejected] into rejects)))))
   {:selected []
    :rejected []}
   site->peers))

(defmethod cts/task-constraints :onyx.task-scheduler/colocated
  [replica jobs task-capacities peer->vm task->node no-op-node job-id]
  (let [task-ids (get-in replica [:tasks job-id])
        capacity (count task-ids)
        boxes (second (first task-capacities))
        n-peers (* capacity boxes)
        site->peers-mapping (large-enough-sites (site->peers replica) capacity)
        peers (mapcat second (into [] site->peers-mapping))
        {:keys [selected rejected]} (select-peers site->peers-mapping n-peers capacity)
        tasks (non-colocated-tasks replica jobs)
        unrestricted-tasks (conj (map task->node tasks) no-op-node)]
    (into
     (reduce
      (fn [result peer-ids]
        (conj result (SplitAmong. (map (comp vector peer->vm) peer-ids)
                                  (map #(vector (get task->node [job-id %])) task-ids))))
      []
      (partition capacity selected))
     (ban-smaller-sites replica jobs peer->vm task->node site->peers-mapping rejected))))
