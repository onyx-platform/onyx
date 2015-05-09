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

(defn current-job-allocations [replica]
  (into {}
        (map (fn [j]
               {j (count (remove nil? (apply concat (vals (get-in replica [:allocations j])))))})
             (:jobs replica))))

(defn current-task-allocations [replica]
  (into
   {}
   (map (fn [j]
          {j (into {}
                   (map (fn [t]
                          {t (count (filter identity (get-in replica [:allocations j t])))})
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
                                   desired (cts/task-distribute-peer-count origin-replica job (get max-utilization job))
                                   tasks (get-in replica [:tasks job])]
                               (map (fn [t]
                                      (when (< (or (get current t) 0) (get desired t))
                                        [job t]))
                                    tasks)))
                           (sort-job-priority replica (:jobs replica))))]
      (if (and (seq peer-pool) (seq candidate-jobs))
        (recur (rest peer-pool)
               (let [removed (common/remove-peers replica (first peer-pool))
                     reset-state (assoc-in removed [:peer-state (first peer-pool)] :warming-up)]
                 (-> reset-state
                     (update-in [:allocations
                                 (ffirst candidate-jobs)
                                 (second (first candidate-jobs))]
                                conj (first peer-pool))
                     (update-in [:allocations
                                 (ffirst candidate-jobs)
                                 (second (first candidate-jobs))]
                                vec))))
        replica))))

(defn find-unused-peers [replica]
  (let [used-peers (apply concat (mapcat vals (vals (get-in replica [:allocations]))))]
    (clojure.set/difference (set (:peers replica)) (set used-peers))))

(defn find-displaced-peers [replica current-allocations max-util]
  (clojure.set/union
   (find-unused-peers replica)
   (remove
    nil?
    (mapcat
     (fn [job]
       (let [overflow (- (get current-allocations job) (get max-util job))]
         (when (pos? overflow)
           (cts/drop-peers replica job overflow))))
     (:jobs replica)))))

(defn exempt-from-acker? [replica job task]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn choose-acker-candidates [replica peers]
  (remove
   (fn [p]
     (let [{:keys [job task]} (common/peer->allocated-job (:allocations replica) p)]
       (exempt-from-acker? replica job task)))
   peers))

(defn choose-ackers [replica]
  ;; TODO: ensure this behaves consistently with respect to ordering
  (reduce
   (fn [result job]
     (let [peers (apply concat (vals (get-in result [:allocations job])))
           pct (or (get-in result [:acker-percentage job]) 10)
           n (int (Math/ceil (* 0.01 pct (count peers))))
           candidates (choose-acker-candidates result peers)]
       (assoc-in result [:ackers job] (vec (take n candidates)))))
   replica
   (:jobs replica)))

(defn deallocate-starved-jobs
  "Strips out allocations from jobs that no longer meet the minimum number
   of peers. This can happen if a peer leaves from a running job."
  [replica]
  (reduce
   (fn [result job]
     (if (< (apply + (map count (vals (get-in result [:allocations job]))))
            (apply + (vals (get-in result [:min-required-peers job]))))
       (update-in result [:allocations] dissoc job)
       result))
   replica
   (:jobs replica)))

(defn reconfigure-cluster-workload [replica]
  (let [job-offers (job-offer-n-peers replica)
        job-claims (job-claim-peers replica job-offers)
        spare-peers (apply + (vals (merge-with - job-offers job-claims)))
        max-utilization (claim-spare-peers replica job-claims spare-peers)
        current-allocations (current-job-allocations replica)
        peers-to-displace (find-displaced-peers replica current-allocations max-utilization)
        deallocated (deallocate-starved-jobs replica)]
    (choose-ackers (reallocate-peers deallocated peers-to-displace max-utilization))))