(ns onyx.scheduling.colocated-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions])
  (:import [org.btrplace.model.constraint Fence SplitAmong Ban]))

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
        site->peers-mapping (large-enough-sites (site->peers replica) capacity)
        n-candidate-peers (apply + (map count (vals site->peers-mapping)))]
    (zipmap task-ids (repeat (int (/ n-candidate-peers capacity))))))

(defn ban-smaller-sites [replica jobs peer->vm task->node large-sites]
  (let [sites (keys large-sites)
        peer-ids ((group-by #(some #{(extensions/get-peer-site replica %)} sites) (:peers replica)) nil)
        jobs (filter #(= (get-in replica [:task-schedulers %]) :onyx.task-scheduler/colocated) jobs)
        nodes (mapcat (fn [job-id] (map (fn [task-id] (task->node [job-id task-id])) (get-in replica [:tasks job-id]))) jobs)]
    (map #(Ban. (peer->vm %) nodes) peer-ids)))

(defn non-colocated-tasks [replica jobs]
  (reduce
   (fn [result job-id]
     (if (not= (get-in replica [:task-schedulers job-id]) :onyx.task-scheduler/colocated)
       (into result (map (fn [task-id] [job-id task-id]) (get-in replica [:tasks job-id])))
       result))
   []
   jobs))

(defmethod cts/task-constraints :onyx.task-scheduler/colocated
  [replica jobs peer->vm task->node no-op-node job-id]
  (let [task-ids (get-in replica [:tasks job-id])
        capacity (count task-ids)
        site->peers-mapping (large-enough-sites (site->peers replica) capacity)
        unrestricted-tasks (conj (map task->node (non-colocated-tasks replica jobs)) no-op-node)]
    (into
     (reduce-kv
      (fn [result peer-site peer-ids]
        (let [peer-sets (partition capacity peer-ids)
              all-peer-sets (partition-all capacity peer-ids)
              rets (map
                    (fn [peers]
                      (SplitAmong. (map (comp vector peer->vm) peers)
                                   (conj (map #(vector (get task->node [job-id %])) task-ids)
                                         unrestricted-tasks)))
                    peer-sets)]
          (if (not= (count (last all-peer-sets)) capacity)
            (into result (into rets (map #(Fence. (peer->vm %) unrestricted-tasks) (last all-peer-sets))))
            (into result rets))))
      []
      site->peers-mapping)
     (ban-smaller-sites replica jobs peer->vm task->node site->peers-mapping))))
