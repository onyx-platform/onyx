(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre :refer [info]]))

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
                        (get-in replica [:min-required-peers job task])))
                    tasks))
      (apply + (vals (get-in replica [:min-required-peers job]))))))

(defn job-coverable? [replica job n]
  (>= n (job-lower-bound replica job)))

(defmulti job-offer-n-peers
  (fn [replica]
    (:job-scheduler replica)))

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(defmulti sort-job-priority
  (fn [replica jobs]
    (:job-scheduler replica)))

(defmethod job-offer-n-peers :default
  [replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod claim-spare-peers :default
  [replica]
  (throw (ex-info (format "Job scheduler %s not recognized" (:job-scheduler replica))
                  {:job-scheduler (:job-scheduler replica)})))

(defmethod sort-job-priority :default
  [replica]
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
     (if true ; TODO: << FIX ME >> (job-coverable? replica j n)
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
                                   desired (cts/task-distribute-peer-count origin-replica job (get max-utilization job))
                                   tasks (get-in replica [:tasks job])]
                               (map (fn [t]
                                      (when (< (get current t 0) (get desired t 0))
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
                     (update-in [:allocations job-id task-id] 
                                (fn [allocation]
                                  (vec (conj allocation peer)))))))
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
        (let [overflow (- (get current-allocations job) (get max-util job))]
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
  (sort-by
   (fn [peer]
     (let [colocated-peers (find-physically-colocated-peers replica peer)
           statuses (map
                     #(let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) %)]
                        (exempt-from-acker? replica job task))
                     colocated-peers)]
       (some #{true} statuses)))
   peers))

(defn choose-acker-candidates [replica peers]
  (sort-acker-candidates
   replica
   (remove
    (fn [p]
      (let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) p)]
        (exempt-from-acker? replica job task)))
    peers)))

(defn choose-ackers [replica]
  (reduce
   (fn [result job]
     (let [peers (sort (replica->job-peers replica job))
           pct (or (get-in result [:acker-percentage job]) 10)
           n (int (Math/ceil (* 0.01 pct (count peers))))
           candidates (choose-acker-candidates result peers)]
       (assoc-in result [:ackers job] (vec (take n candidates)))))
   replica
   (:jobs replica)))

(defn remove-job [replica job]
  (let [peers (sort (replica->job-peers replica job))] 
    (-> replica
        (update-in [:allocations] dissoc job)
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

(defmulti equivalent-allocation?
  (fn [replica replica-new]
    (:job-scheduler replica)))

(defmethod equivalent-allocation? :default
  [_ _]
  false)

(defn reconfigure-cluster-workload [replica]
  ;; TODO: << FIX ME >>
  #_(let [job-offers (job-offer-n-peers replica)
          job-claims (job-claim-peers replica job-offers)
          spare-peers (apply + (vals (merge-with - job-offers job-claims)))
          max-utilization (claim-spare-peers replica job-claims spare-peers)
          current-allocations (current-job-allocations replica)
          peers-to-displace (find-displaced-peers replica current-allocations max-utilization)
          updated-replica (choose-ackers (reallocate-peers replica peers-to-displace max-utilization))
          final-replica updated-replica ; TODO: << FIX ME >> (deallocate-starved-jobs updated-replica)
          ]
      (if (equivalent-allocation? replica final-replica)
        replica
        final-replica))

  (if-let [jobs (:jobs replica)]
    (let [target-job (first jobs)
          [task-1 task-2 task-3] (get-in replica [:tasks target-job])
          target-peer (first (:peers replica))]
      
      (cond (:done? replica)
            replica

            (and (nil? (:task (common/peer->allocated-job (:allocations replica) target-peer))) (not (:done? replica)))
            (-> replica
                (update-in [:allocations target-job task-1] #(vec (conj % target-peer))))

            (= task-1 (:task (common/peer->allocated-job (:allocations replica) target-peer)))
            (do ;(prn (get-in replica [:partitions target-job]))
                (if-let [parts (get-in replica [:partitions target-job task-1])]
                  (let [part (inc (get-in replica [:assigned-partition target-job task-1 target-peer] -1))]
                    (if (< part parts)
                      (-> replica
                          (update-in [:allocations target-job task-1] #(vec (conj % target-peer)))
                          (assoc-in [:assigned-partition target-job task-1 target-peer] part))
                      (-> replica
                          (assoc-in [:allocations target-job task-1] [])
                          (update-in [:allocations target-job task-2] #(vec (conj % target-peer)))
                          (assoc-in [:assigned-partition target-job task-2 target-peer] 0))))
                  (update-in replica [:allocations target-job task-1] #(vec (conj % target-peer)))))

            (= task-2 (:task (common/peer->allocated-job (:allocations replica) target-peer)))
            (if-let [parts (get-in replica [:partitions target-job task-2])]
              (let [part (inc (get-in replica [:assigned-partition target-job task-2 target-peer] -1))]
                (if (< part parts)
                  (-> replica
                      (update-in [:allocations target-job task-2] #(vec (conj % target-peer)))
                      (assoc-in [:assigned-partition target-job task-2 target-peer] part))
                  (-> replica
                      (assoc-in [:allocations target-job task-2] [])
                      (update-in [:allocations target-job task-3] #(vec (conj % target-peer)))
                      (assoc-in [:assigned-partition target-job task-3 target-peer] 0))))
              (update-in replica [:allocations target-job task-2] #(vec (conj % target-peer))))

            (= task-3 (:task (common/peer->allocated-job (:allocations replica) target-peer)))
            (if-let [parts (get-in replica [:partitions target-job task-3])]
              (let [part (inc (get-in replica [:assigned-partition target-job task-3 target-peer] -1))]
                (if (< part parts)
                  (-> replica
                      (update-in [:allocations target-job task-3] #(vec (conj % target-peer)))
                      (assoc-in [:assigned-partition target-job task-3 target-peer] part))
                  (-> replica
                      (assoc :done? true)
                      (assoc-in [:allocations target-job task-3] [])
                      (update-in [:completed-jobs] #(vec (conj % target-job))))))
              (update-in replica [:allocations target-job task-3] #(vec (conj % target-peer))))

            :else
            (throw (ex-info "Hit the bottom" {}))))
    replica))
