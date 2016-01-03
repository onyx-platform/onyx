(ns onyx.scheduling.colocated-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.log.commands.common :as common]
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

(defmethod cts/task-distribute-peer-count :onyx.task-scheduler/colocated
  [replica job-id n]
  (let [task-ids (get-in replica [:tasks job-id])
        capacity (count task-ids)
        site->peers-mapping (large-enough-sites (site->peers replica) capacity)]
    (if (seq site->peers-mapping)
      (zipmap task-ids (repeat (int (/ n capacity))))
      (zipmap task-ids (repeat 0)))))

(defn ban-smaller-sites [replica jobs peer->vm task->node large-sites]
  (let [sites (keys large-sites)
        peer-ids ((group-by #(some #{(extensions/get-peer-site replica %)} sites) (:peers replica)) nil)
        jobs (filter #(= (get-in replica [:task-schedulers %]) :onyx.task-scheduler/colocated) jobs)
        nodes (mapcat (fn [job-id] (map (fn [task-id] (task->node [job-id task-id])) (get-in replica [:tasks job-id]))) jobs)]
    (map #(Ban. (peer->vm %) nodes) peer-ids)))

(defmethod cts/task-constraints :onyx.task-scheduler/colocated
  [replica jobs peer->vm task->node no-op-node job-id]
  (let [task-ids (get-in replica [:tasks job-id])
        capacity (count task-ids)
        site->peers-mapping (large-enough-sites (site->peers replica) capacity)]
    (into
     (reduce-kv
      (fn [result peer-site peer-ids]
        (conj result
              (SplitAmong. (map (comp vector peer->vm) peer-ids)
                           (conj (map #(vector (get task->node [job-id %])) task-ids)
                                 [no-op-node]))))
      []
      site->peers-mapping)
     (ban-smaller-sites replica jobs peer->vm task->node site->peers-mapping))))
