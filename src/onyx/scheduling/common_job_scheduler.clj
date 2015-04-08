(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre :refer [info]]))

(defmulti job-offer-n-peers
  (fn [replica]
    (:job-scheduler replica)))

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(defn current-job-allocations [replica]
  (into {}
        (map (fn [j]
               {j (apply + (map count (vals (get-in replica [:allocations j]))))})
             (:jobs replica))))

(defn current-task-allocations [replica]
  (into
   {}
   (map (fn [j]
          {j (into {} (map (fn [[t a]] {t (count a)}) (get-in replica [:allocations j])))})
        (:jobs replica))))

(defn job->task-claims [replica job-offers]
  (reduce-kv
   (fn [all j claim]
     (assoc all j (cts/task-claim-n-peers replica j claim)))
   {}
   job-offers))

(defn reallocate-peers [origin-replica displaced-peers max-utilization]
  (loop [peer-pool displaced-peers
         replica origin-replica]
    (let [candidate-jobs (filter identity
                                 (mapcat
                                  (fn [job]
                                    (let [current (get (current-task-allocations replica) job)
                                          desired (cts/task-distribute-peer-count replica job (get max-utilization job))
                                          tasks (get-in replica [:tasks job])]
                                      (map
                                       (fn [t]
                                         (when (< (or (get current t) 0) (get desired t))
                                           [job t]))
                                       tasks)))
                                  (:jobs replica)))]
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
   (mapcat
    (fn [job]
      (let [overflow (- (get current-allocations job) (get max-util job))]
        (when (pos? overflow)
          (cts/drop-peers replica job overflow))))
    (:jobs replica))))

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
           pct (get-in result [:acker-percentage job] 10)
           n (int (Math/ceil (* (* 0.01 pct) (count peers))))
           candidates (choose-acker-candidates result peers)]
       (assoc-in result [:ackers job] (vec (take n candidates)))))
   replica
   (:jobs replica)))

(defn reconfigure-cluster-workload [replica]
  (let [job-offers (job-offer-n-peers replica)
        job-claims (job->task-claims replica job-offers)
        spare-peers (apply + (vals (merge-with - job-offers job-claims)))
        max-utilization (claim-spare-peers replica job-claims spare-peers)
        current-allocations (current-job-allocations replica)
        peers-to-displace (find-displaced-peers replica current-allocations max-utilization)]
    (choose-ackers (reallocate-peers replica peers-to-displace max-utilization))))
