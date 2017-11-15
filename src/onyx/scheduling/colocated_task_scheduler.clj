(ns onyx.scheduling.colocated-task-scheduler
  (:require [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.log.commands.common :as common]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.extensions :as extensions])
  (:import [org.btrplace.model.constraint Fence SplitAmong Ban]))

(defn get-peer-site [replica p]
  (get-in replica [:peer-sites p]))

(defn site->peers [replica]
  (group-by
   (fn [p] (get-peer-site replica p))
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
        (remove nil?
                (map #(get-in replica [:task-saturation job-id %])
                     task-ids))]
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

(defn ban-smaller-sites [replica jobs peer->vm task->node large-sites rejected]
  (let [sites (keys large-sites)
        peer-ids (into
                  ((group-by #(some #{(get-peer-site replica %)} sites)
                             (:peers replica))
                   nil)
                  rejected)
        jobs (filter #(= (get-in replica [:task-schedulers %])
                         :onyx.task-scheduler/colocated)
                     jobs)
        nodes (mapcat
               (fn [job-id]
                 (map
                  (fn [task-id]
                    (task->node [job-id task-id]))
                  (get-in replica [:tasks job-id])))
               jobs)]
    (map #(Ban. (peer->vm %) nodes) peer-ids)))

(defn non-colocated-tasks [replica jobs]
  (reduce
   (fn [result job-id]
     (if (not= (get-in replica [:task-schedulers job-id])
               :onyx.task-scheduler/colocated)
       (into result
             (map
              (fn [task-id] [job-id task-id])
              (get-in replica [:tasks job-id])))
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
             sel (take difference (drop-last (mod (count peer-ids) size) peer-ids))
             rej (take-last (mod (count peer-ids) size) peer-ids)]
         (-> result
             (update-in [:selected] into sel)
             (update-in [:rejected] into rej)))))
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
        unrestricted-tasks (conj (map task->node (non-colocated-tasks replica jobs)) no-op-node)]
    (into
     (reduce
      (fn [result peer-ids]
        (conj result
              (SplitAmong.
               (map (comp vector peer->vm) peer-ids)
               (map #(vector (get task->node [job-id %])) task-ids))))
      []
      (partition capacity selected))
     (ban-smaller-sites replica jobs peer->vm task->node site->peers-mapping rejected))))

(defmethod cts/assign-capacity-constraint? :onyx.task-scheduler/colocated
  [replica job-id]
  false)

(defn find-colocated-peers [replica this-peer other-peers]
  (let [my-site (m/get-peer-site replica this-peer)]
    (filter #(= my-site (m/get-peer-site replica %)) other-peers)))

(defn choose-candidates [replica peer-config this-peer downstream-peers]
  (if (:onyx.task-scheduler.colocated/only-send-local? peer-config)
    (find-colocated-peers replica this-peer downstream-peers)
    downstream-peers))

(defmethod cts/choose-downstream-peers :onyx.task-scheduler/colocated
  [replica job-id peer-config this-peer downstream-peers]
  (let [candidates (choose-candidates replica peer-config this-peer downstream-peers)]
    (fn [hash-group]
      (rand-nth candidates))))

(defmethod cts/choose-acker :onyx.task-scheduler/colocated
  [replica job-id peer-config this-peer ackers]
  (let [candidates (choose-candidates replica peer-config this-peer ackers)]
    (if (not (seq candidates))
      (throw
       (ex-info
        (format
         "Job %s does not have an acker per machine, which is needed for the colocated task scheduler. Raise the limit via the job parameter :acker/percentage." job-id)
        {}))
      (fn []
        (rand-nth candidates)))))
