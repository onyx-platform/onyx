(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [onyx.scheduling.acker-scheduler :refer [choose-ackers]])
  (:import [org.btrplace.model Model DefaultModel]
           [org.btrplace.model.constraint Running RunningCapacity Quarantine Fence]
           [org.btrplace.scheduler.choco DefaultChocoScheduler DefaultParameters]))

(defn job-upper-bound [replica job]
  ;; We need to handle a special case here when figuring out the upper saturation limit.
  ;; If this is a job with a grouped task that has already been allocated,
  ;; we can't allocate to the grouped task anymore, even if it's saturation
  ;; level is Infinity.
  (let [tasks (get-in replica [:tasks job])
        grouped-tasks (filter (fn [task] (get-in replica [:flux-policies job task])) tasks)]
    (if (seq (filter (fn [task] (seq (get-in replica [:allocations job task]))) grouped-tasks))
      (apply + (map (fn [task]
                      (if (some #{task} grouped-tasks)
                        ;; Cannot allocate anymore, you have what you have.
                        (count (get-in replica [:allocations job task]))
                        ;; Allocate as much the original task saturation allows since it hasn't
                        ;; been allocated yet.
                        (get-in replica [:task-saturation job task])))
                    tasks))
      (get-in replica [:saturation job] Double/POSITIVE_INFINITY))))

(defn job-lower-bound [replica job]
  ;; Again, we handle the special case of a grouped task that has already
  ;; begun.
  (let [tasks (get-in replica [:tasks job])
        grouped-tasks (filter (fn [task] (get-in replica [:flux-policies job task])) tasks)]
    (if (seq (filter (fn [task] (seq (get-in replica [:allocations job task]))) grouped-tasks))
      (apply + (map (fn [task]
                      (if (some #{task} grouped-tasks)
                        ;; Cannot allocate anymore, you have what you have.
                        (count (get-in replica [:allocations job task]))
                        ;; Grab the absolute minimum for this task, no constraints.
                        (get-in replica [:min-required-peers job task] 1)))
                    tasks))
      (apply + (vals (get-in replica [:min-required-peers job]))))))

(defn job-coverable? [replica job n]
  (>= n (job-lower-bound replica job)))

(defn job->planned-task-capacity [replica jobs-ids utilization]
  (reduce
   (fn [result job-id]
     (let [job-capacity (get utilization job-id 0)
           task-capacity (cts/task-distribute-peer-count replica job-id job-capacity)]
       (assoc result job-id task-capacity)))
   {}
   jobs-ids))

(defmulti job-offer-n-peers
  (fn [replica jobs]
    (:job-scheduler replica)))

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(defmethod job-offer-n-peers :default
  [replica jobs]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod claim-spare-peers :default
  [replica jobs n]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defn job-claim-peers [replica job-offers]
  (reduce-kv
   (fn [all j n]
     (if (job-coverable? replica j n)
       (let [sat (job-upper-bound replica j)]
         (assoc all j (min sat n)))
       (assoc all j 0)))
   {}
   job-offers))

(defn remove-job [replica job]
  (let [peers (sort (common/replica->job-peers replica job))]
    (-> replica
        (update-in [:allocations] dissoc job)
        (update-in [:task-slot-ids] dissoc job)
        (update-in [:peer-state] (fn [peer-state]
                                   (reduce (fn [ps peer]
                                             (assoc ps peer :idle))
                                           peer-state
                                           peers))))))

(defn deallocate-starved-jobs
  "Strips out allocations from jobs that no longer meet the minimum number
   of peers. This can happen if a peer leaves from a running job."
  [replica]
  (reduce
   (fn [result job]
     (let [tasks (get-in replica [:tasks job])]
       (if (every? (fn [t]
                     (>= (count (get-in result [:allocations job t]))
                         (get-in result [:min-required-peers job t] 1)))
                   tasks)
         result
         (remove-job result job))))
   replica
   (:jobs replica)))

(defn full-allocation? [replica utilization planned-capacities]
  (reduce
   (fn [result job-id]
     (let [tasks (get-in replica [:tasks job-id])
           capacities (get planned-capacities job-id)]
       (if (every?
            #(= (count (get-in replica [:allocations job-id %]))
                (get capacities %))
            tasks)
         true
         (reduced false))))
   true
   (keys utilization)))

(defn unrolled-tasks [replica task-utilization]
  (mapcat
   (fn [job-id]
     (map #(-> [job-id %]) (get-in replica [:tasks job-id])))
   (keys task-utilization)))

(defn build-peer->vm [replica model]
  (let [mapping (.getMapping model)]
    (reduce
     (fn [result peer-id]
       (let [vm (.newVM model)]
         (.addReadyVM mapping vm)
         (assoc result peer-id vm)))
     {}
     (:peers replica))))

(defn build-job-and-task->node [model task-seq]
  (let [mapping (.getMapping model)]
    (reduce
     (fn [result [job-id task-id]]
       (let [node (.newNode model)]
         (.addOnlineNode mapping node)
         (assoc result [job-id task-id] node)))
     {}
     task-seq)))

(defn build-node->task [task->node]
  (reduce-kv
   (fn [result job-and-task-ids node]
     (assoc result node job-and-task-ids))
   {}
   task->node))

(defn build-peer->task [result-model peer->vm node->task]
  (reduce-kv
   (fn [all peer-id btr-vm]
     (let [node (.getVMLocation (.getMapping result-model) btr-vm)]
       (if-let [task (get node->task node)]
         (assoc all peer-id task)
         (assoc all peer-id nil))))
   {}
   peer->vm))

(defn peer-running-constraints [peer->vm]
  (map #(Running. %) (vals peer->vm)))

(defn calculate-capacity
  [replica task-capacities task->node [job-id task-id :as id]]
  (if (= (get-in replica [:flux-policies job-id task-id]) :recover)
    ;; :recover mode tries to bring its peer count back to its original
    ;; number of peers - which is reliably captured in :min-required-peers.
    (get-in replica [:min-required-peers job-id task-id])
    (get task-capacities task-id)))

(defn capacity-constraints
  [replica job-utilization task-seq task->node planned-capacities]
  (reduce
   (fn [result [job-id task-id :as id]]
     (cond (zero? (get job-utilization job-id))
           (conj result (RunningCapacity. (get task->node id) 0))

           (cts/assign-capacity-constraint? replica job-id)
           (let [capacities (get planned-capacities job-id)
                 n (calculate-capacity replica capacities task->node id)]
             (conj result (RunningCapacity. (get task->node id) n)))

           :else
           result))
   []
   task-seq))

(defn grouping-task-constraints [replica task-seq task->node peer->vm]
  (reduce
   (fn [result [job-id task-id :as id]]
     (let [flux-policy (get-in replica [:flux-policies job-id task-id])
           peers (seq (get-in replica [:allocations job-id task-id]))]
       (cond (and (= flux-policy :recover) peers)
             ;; BtrPlace Fence constraint means we can add more
             ;; peers to this task, but the peers we already added
             ;; cannot leave this task.
             (into result (map #(Fence. (get peer->vm %)
                                        [(get task->node id)])
                               peers))
             (and flux-policy peers)
             ;; BtrPlace Quarantine constraint means no new peers
             ;; can be added or removed from this task. We set
             ;; the peers that are already on this task by
             ;; registering them directly through the Mapping.
             (conj result (Quarantine. (get task->node id)))
             :else result)))
   []
   task-seq))

(defn update-peer-site [replica task-id peer-id]
  (update-in replica [:peer-sites peer-id]
             (fn [peer-site]
               (let [resources (extensions/assign-task-resources
                                replica
                                peer-id
                                task-id
                                peer-site
                                (:peer-sites replica))]
                 (merge peer-site resources)))))

(defn assign-task-resources [new-replica original-replica peer->task]
  (reduce-kv
   (fn [result peer-id [job-id task-id]]
     (if (and job-id task-id)
       (let [prev-task (get-in original-replica [:allocations job-id task-id])]
         (if-not (some #{peer-id} prev-task)
           (update-peer-site result task-id peer-id)
           result))
       result))
   new-replica
   peer->task))

(defn update-slot-id-for-peer [replica job-id task-id peer-id]
  (update-in replica [:task-slot-ids job-id task-id]
             (fn [slot-ids]
               (let [slot-id (first (remove (set (vals slot-ids)) (range)))]
                 (assoc slot-ids peer-id slot-id)))))

(defn assign-task-slot-ids [new-replica original-replica peer->task]
  (reduce-kv
   (fn [result peer-id [job-id task-id]]
     (if (and job-id task-id)
       (let [prev-task (get-in original-replica [:allocations job-id task-id])]
         (if-not (some #{peer-id} prev-task)
           (if-let [prev-allocation (common/peer->allocated-job (:allocations original-replica) peer-id)]
             (let [prev-job-id (:job prev-allocation)
                   prev-task-id (:task prev-allocation)]
               (-> result
                   (update-in [:task-slot-ids prev-job-id prev-task-id] dissoc peer-id)
                   (update-slot-id-for-peer job-id task-id peer-id)))
             (update-slot-id-for-peer result job-id task-id peer-id))
           result))
       (if-let [prev-allocation (common/peer->allocated-job (:allocations original-replica) peer-id)]
         (let [prev-job-id (:job prev-allocation)
               prev-task-id (:task prev-allocation)]
           (update-in result [:task-slot-ids prev-job-id prev-task-id] dissoc peer-id))
         result)))
   new-replica
   peer->task))

(defn build-current-model [replica mapping task->node peer->vm]
  (doseq [j (:jobs replica)]
    (doseq [t (keys (get-in replica [:allocations j]))]
      (let [node (get task->node [j t])]
        (doseq [p (get-in replica [:allocations j t])]
          (let [vm (get peer->vm p)]
            (.addRunningVM mapping vm node)))))))

(defn change-peer-state [new-replica original-replica peer->task]
  (reduce-kv
   (fn [result peer-id [job-id task-id]]
     (let [prev-task (get-in original-replica [:allocations job-id task-id])]
       (if-not (some #{peer-id} prev-task)
         (assoc-in result [:peer-state peer-id] :idle)
         result)))
   new-replica
   peer->task))

(defn change-peer-allocations [replica peer->task]
  (let [allocations
        (reduce-kv
         (fn [result peer-id [job-id task-id :as id]]
           (if (and job-id task-id)
             (update-in result id (comp vec conj) peer-id)
             result))
         {}
         peer->task)]
    (assoc replica :allocations allocations)))

(defn n-unused-peers [replica task-seq capacities]
  (max (- (count (:peers replica))
          (reduce
           (fn [result [job-id task-id :as id]]
             (let [task-capacity (get capacities job-id)
                   capacity (get task-capacity task-id)]
               (+ result capacity)))
           0
           task-seq))
       0))

(defn no-op-constraints [replica task-seq capacities no-op-node]
  [(RunningCapacity. no-op-node (n-unused-peers replica task-seq capacities))])

(defn task-constraints [replica jobs capacities peer->vm task->node no-op-node]
  (mapcat
   #(cts/task-constraints
     replica jobs (get capacities % 0) peer->vm task->node no-op-node %)
   jobs))

(defn btr-place-scheduling [replica jobs job-utilization capacities]
  (if (seq jobs)
    (let [model (DefaultModel.)
          ;; Hard code the random seed to make it deterministic
          ;; across peers.
          params (.setRandomSeed (DefaultParameters.) 1)
          scheduler (DefaultChocoScheduler. params)
          mapping (.getMapping model)
          task-seq (unrolled-tasks replica job-utilization)
          peer->vm (build-peer->vm replica model)
          task->node (build-job-and-task->node model task-seq)
          no-op-node (.newNode model)]
      (.addOnlineNode mapping no-op-node)
      (build-current-model replica mapping task->node peer->vm)
      (let [node->task (build-node->task task->node)
            capacity-cts (capacity-constraints replica job-utilization task-seq task->node capacities)
            running-cts (peer-running-constraints peer->vm)
            grouping-cts (grouping-task-constraints replica task-seq task->node peer->vm)
            no-op-cts (no-op-constraints replica task-seq capacities no-op-node)
            task-cts (task-constraints replica jobs capacities peer->vm task->node no-op-node)
            constraints (reduce into [capacity-cts running-cts grouping-cts no-op-cts task-cts])
            plan (.solve scheduler model constraints)]
        (when plan
          (let [result-model (.getResult plan)
                peer->task (build-peer->task result-model peer->vm node->task)
                original-replica replica]
            (-> replica
                (change-peer-allocations peer->task)
                (change-peer-state original-replica peer->task)
                (assign-task-resources original-replica peer->task)
                (assign-task-slot-ids original-replica peer->task))))))
    replica))

(defn reconfigure-cluster-workload [replica]
  (loop [jobs (:jobs replica)
         current-replica replica]
    (if (not (seq jobs))
      current-replica
      (let [job-offers (job-offer-n-peers current-replica jobs)
            job-claims (job-claim-peers current-replica job-offers)
            spare-peers (apply + (vals (merge-with - job-offers job-claims)))
            max-utilization (claim-spare-peers current-replica job-claims spare-peers)
            planned-capacities (job->planned-task-capacity current-replica jobs max-utilization)
            updated-replica (btr-place-scheduling current-replica jobs max-utilization planned-capacities)]
        (if updated-replica
          (let [acker-replica (choose-ackers updated-replica jobs)]
            (if (full-allocation? acker-replica max-utilization planned-capacities)
              (deallocate-starved-jobs acker-replica)
              (recur (butlast jobs) (remove-job current-replica (butlast jobs)))))
          (recur (butlast jobs) (remove-job current-replica (butlast jobs))))))))
