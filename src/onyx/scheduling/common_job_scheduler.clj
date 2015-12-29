(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre :refer [info]])
  (:import [org.btrplace.model Model DefaultModel]
           [org.btrplace.model.view ShareableResource]
           [org.btrplace.model.constraint Running Among RunningCapacity
            Quarantine Fence]
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

(defmulti job-offer-n-peers
  (fn [replica jobs]
    (:job-scheduler replica)))

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(defmulti sort-job-priority
  (fn [replica jobs]
    (:job-scheduler replica)))

(defmethod job-offer-n-peers :default
  [replica jobs]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod claim-spare-peers :default
  [replica jobs n]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod sort-job-priority :default
  [replica jobs]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defn replica->job-peers [replica job-id]
  (apply concat (vals (get-in replica [:allocations job-id]))))

(defn current-job-allocations [replica]
  (into {}
        (map (fn [j]
               {j (count (remove nil? (replica->job-peers replica j)))})
             (:jobs replica))))

(defn current-task-allocations [replica]
  (into
   {}
   (map (fn [j]
          {j (into {}
                   (map (fn [t]
                          {t (count (remove nil? (get-in replica [:allocations j t])))})
                        (get-in replica [:tasks j])))})
        (:jobs replica))))

(defn job-claim-peers [replica job-offers]
  (reduce-kv
   (fn [all j n]
     (if (job-coverable? replica j n)
       (let [sat (job-upper-bound replica j)]
         (assoc all j (min sat n)))
       (assoc all j 0)))
   {}
   job-offers))

(defn reallocate-peers [origin-replica displaced-peers max-utilization]
  (loop [peer-pool displaced-peers
         replica origin-replica]
    (let [candidate-jobs (remove
                          nil?
                          (mapcat
                           (fn [job]
                             (let [current (get (current-task-allocations replica) job)
                                   desired (cts/task-distribute-peer-count origin-replica job (get max-utilization job 0))
                                   tasks (get-in replica [:tasks job])]
                               (map (fn [t]
                                      (when (< (get current t 0) (get desired t))
                                        [job t]))
                                    tasks)))
                           (sort-job-priority replica (:jobs replica))))]
      (if (and (seq peer-pool) (seq candidate-jobs))
        (recur (rest peer-pool)
               (let [peer (first peer-pool)
                     [job-id task-id] (first candidate-jobs)]
                 (-> replica
                     (common/remove-peers peer)
                     (assoc-in [:peer-state peer] :idle)
                     (update-in [:peer-sites peer] (fn [peer-site] 
                                                     (merge peer-site 
                                                            (extensions/assign-task-resources replica
                                                                                              peer
                                                                                              task-id
                                                                                              peer-site
                                                                                              (:peer-sites replica)))))
                     (update-in [:allocations job-id task-id]
                                (fn [allocation]
                                  (vec (conj allocation peer))))
                     (update-in [:task-slot-ids job-id task-id]
                                (fn [slot-ids]
                                  (let [slot-id (first 
                                                  (remove (set (vals slot-ids))
                                                          (range)))]
                                    (assoc slot-ids peer slot-id)))))))
        replica))))

(defn find-unused-peers [replica]
  (let [used-peers (apply concat (mapcat vals (vals (get-in replica [:allocations]))))]
    (clojure.set/difference (set (:peers replica)) (set used-peers))))

(defn find-displaced-peers [replica current-allocations max-util]
  (distinct
   (concat
    (find-unused-peers replica)
    (remove
     nil?
     (mapcat
      (fn [job]
        (let [overflow (- (get current-allocations job 0) (get max-util job 0))]
          (when (pos? overflow)
            (cts/drop-peers replica job overflow))))
      (:jobs replica))))))

(defn exempt-from-acker? [replica job task]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn find-physically-colocated-peers
  "Takes replica and a peer. Returns a set of peers, exluding this peer,
   that reside on the same physical machine."
  [replica peer]
  (let [peers (remove (fn [p] (= p peer)) (:peers replica))
        peer-site (extensions/get-peer-site replica peer)]
    (filter
     (fn [p]
       (= (extensions/get-peer-site replica p) peer-site))
     peers)))

(defn sort-acker-candidates
  "We try to be smart about which ackers we pick. If we can avoid
   colocating an acker and any peers executing an exempt task,
   we try to. It's a best effort, though, so if it's not possible
   we proceed anyway."
  [replica peers]
  (let [preferences (map (fn [peer]
                           (let [colocated-peers (find-physically-colocated-peers replica peer)
                                 statuses (map #(let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) %)]
                                                  (exempt-from-acker? replica job task))
                                               colocated-peers)]
                             (some #{true} statuses)))
                         peers)]
    (->> peers
         (map list preferences)
         (sort-by first)
         (map second))))

(defn choose-acker-candidates [replica peers]
  (sort-acker-candidates
   replica
   (remove
    (fn [p]
      (let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) p)]
        (exempt-from-acker? replica job task)))
    peers)))

(defn choose-ackers [replica jobs]
  (reduce
   (fn [result job]
     (let [peers (sort (replica->job-peers replica job))
           pct (or (get-in result [:acker-percentage job]) 10)
           n (int (Math/ceil (* 0.01 pct (count peers))))
           candidates (choose-acker-candidates result peers)]
       (assoc-in result [:ackers job] (vec (take n candidates)))))
   replica
   jobs))

(defn remove-job [replica job]
  (let [peers (sort (replica->job-peers replica job))]
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

(defn full-allocation? [replica utilization]
  (reduce
   (fn [result job-id]
     (let [tasks (get-in replica [:tasks job-id])
           capacities (cts/task-distribute-peer-count replica job-id (get utilization job-id 0))]
       (if (every?
            #(= (count (get-in replica [:allocations job-id %]))
                (get capacities %))
            tasks)
         true
         (reduced false))))
   true
   (keys utilization)))

(defmulti equivalent-allocation?
  (fn [replica replica-new]
    (:job-scheduler replica)))

(defmethod equivalent-allocation? :default
  [_ _]
  false)

(defn unrolled-tasks [replica task-utilization]
  (mapcat
   (fn [job-id]
     (map #(-> [job-id %]) (get-in replica [:tasks job-id])))
   (keys task-utilization)))

(defn n-peers-running [replica job-utilization]
  (reduce
   (fn [sum job-id]
     (let [n (get job-utilization job-id 0)]
       (+ sum (apply + (vals (cts/task-distribute-peer-count replica job-id n))))))
    0
   (keys job-utilization)))

(defn build-peer->vm [replica model mapping job-utilization]
  (let [n-peers (n-peers-running replica job-utilization)
        running-peers (reduce into [] (mapcat vals (vals (:allocations replica))))
        allocated-vms (reduce
                       (fn [result peer-id]
                         (let [vm (.newVM model)]
                           (.addReadyVM mapping vm)
                           (assoc result peer-id vm)))
                       {}
                       running-peers)]
    (if (< (count running-peers) n-peers)
      (let [n-more-required (- n-peers (count running-peers))
            unused-peers (remove #(some #{%} running-peers) (:peers replica))
            extra-peers (take n-more-required unused-peers)]
        (into
         allocated-vms
         (reduce
          (fn [result peer-id]
            (let [vm (.newVM model)]
              (.addReadyVM mapping vm)
              (assoc result peer-id vm)))
          allocated-vms
          extra-peers)))
      allocated-vms)))

(defn build-job-and-task->node [model mapping task-seq]
  (reduce
   (fn [result [job-id task-id]]
     (let [node (.newNode model)]
       (.addOnlineNode mapping node)
       (assoc result [job-id task-id] node)))
   {}
   task-seq))

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
       (assoc all peer-id (get node->task node))))
   {}
   peer->vm))

(defn peer-running-constraints [peer->vm]
  (map #(Running. %) (vals peer->vm)))

(defn capacity-constraints [replica task-utilization task-seq task->node]
  (map
   (fn [[job-id task-id :as id]]
     (let [utilization (get task-utilization job-id 0)
           capacities (cts/task-distribute-peer-count replica job-id utilization)]
       (if (= (get-in replica [:flux-policies job-id task-id]) :recover)
         (let [n-peers (get-in replica [:min-required-peers job-id task-id])]
           (RunningCapacity. (get task->node id) n-peers))
         (let [n-peers (get capacities task-id)]
           (RunningCapacity. (get task->node id) n-peers)))))
   task-seq))

(defn grouping-task-constraints [replica task-seq task->node peer->vm]
  (reduce
   (fn [result [job-id task-id :as id]]
     (let [flux-policy (get-in replica [:flux-policies job-id task-id])
           peers (seq (get-in replica [:allocations job-id task-id]))]
       (cond (and (= flux-policy :recover) peers)
             (into result (map #(Fence. (get peer->vm %)
                                        [(get task->node id)])
                               peers))
             (and flux-policy peers)
             (conj result (Quarantine. (get task->node id)))
             :else result)))
   []
   task-seq))

(defn assign-task-resources [replica peer->task]
  (reduce-kv
   (fn [result peer-id [job-id task-id]]
     (update-in result [:peer-sites peer-id]
                (fn [peer-site]
                  (let [resources (extensions/assign-task-resources
                                   result
                                   peer-id
                                   task-id
                                   peer-site
                                   (:peer-sites result))]
                    (merge peer-site resources)))))
   replica
   peer->task))

(defn assign-task-slot-ids [replica peer->task]
  (reduce-kv
   (fn [result peer-id [job-id task-id]]
     (update-in result [:task-slot-ids job-id task-id]
                (fn [slot-ids]
                  (let [slot-id (first (remove (set (vals slot-ids)) (range)))]
                    (assoc slot-ids peer-id slot-id)))))
   replica
   peer->task))

(defn build-current-model [replica mapping task->node peer->vm]
  (doseq [j (:jobs replica)]
    (doseq [t (keys (get-in replica [:allocations j]))]
      (let [node (get task->node [j t])]
        (doseq [p (get-in replica [:allocations j t])]
          (let [vm (get peer->vm p)]
            (.addRunningVM mapping vm node)))))))

(defn btr-place-scheduling [replica job-utilization]
  (if (seq (:jobs replica))
    (let [model (DefaultModel.)
          ;; Hard code the random seed to make it deterministic
          ;; across peers.
          params (.setRandomSeed (DefaultParameters.) 1)
          scheduler (DefaultChocoScheduler. params)
          mapping (.getMapping model)
          task-seq (unrolled-tasks replica job-utilization)
          peer->vm (build-peer->vm replica model mapping job-utilization)
          task->node (build-job-and-task->node model mapping task-seq)]
      (build-current-model replica mapping task->node peer->vm)
      (let [node->task (build-node->task task->node)
            capacity-constraints (capacity-constraints replica job-utilization task-seq task->node)
            running-constraints (peer-running-constraints peer->vm)
            grouping-constraints (grouping-task-constraints replica task-seq task->node peer->vm)
            constraints (into (into capacity-constraints running-constraints) grouping-constraints)
            plan (.solve scheduler model constraints)]
        (when plan
          (let [result-model (.getResult plan)
                peer->task (build-peer->task result-model peer->vm node->task)]
            (-> replica
                (assoc :allocations (reduce-kv #(update-in %1 %3 (comp vec conj) %2) {} peer->task))
                (assoc :peer-state (reduce #(assoc %1 %2 :idle) {} (:peers replica)))
                (assign-task-resources peer->task)
                (assign-task-slot-ids peer->task))))))
    replica))

;;; [x] remove resource limits on nodes.
;;; [x] add constant seed for generators
;;; [x] Grouping recovery flux policy
;;; [x] handle planning not coming up with a solution
;;; [x] skip jobs that don't get enough peers.
;;; [x] don't try to allocate all the peers
;;; [ ] don't make all peers idle
;;; [ ] don't change all task slots
;;; [ ] don't reallocate all task resources
;;; [x] fix jitter

(defn reconfigure-cluster-workload [replica]
  (loop [jobs (:jobs replica)]
    (if (not (seq jobs))
      replica
      (let [job-offers (job-offer-n-peers replica jobs)
            job-claims (job-claim-peers replica job-offers)
            spare-peers (apply + (vals (merge-with - job-offers job-claims)))
            max-utilization (claim-spare-peers replica job-claims spare-peers)
            updated-replica (btr-place-scheduling replica max-utilization)
;            acker-replica (choose-ackers updated-replica jobs)
            ]
        (if (and updated-replica
                 (full-allocation? updated-replica max-utilization))
          updated-replica
          (recur (butlast jobs)))))))
