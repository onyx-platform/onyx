(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-task-scheduler :as cts]
            [taoensso.timbre]))

(defmulti job-offer-n-peers
  (fn [replica]
    (:job-scheduler replica)))

(defmulti claim-spare-peers
  (fn [replica jobs n]
    (:job-scheduler replica)))

(defn at-least-one-active? [replica peers]
  (->> peers
       (map #(get-in replica [:peer-state %]))
       (filter (partial = :active))
       (seq)))

(defn job-covered? [replica job]
  (let [tasks (get-in replica [:tasks job])
        active? (partial at-least-one-active? replica)]
    (every? identity (map #(active? (get-in replica [:allocations job %])) tasks))))

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
                                         (when (< (get current t) (get desired t))
                                           [job t]))
                                       tasks)))
                                  (:jobs replica)))]
      (if (and (seq peer-pool) (seq candidate-jobs))
        (recur (rest peer-pool)
               (update-in replica [:allocations
                                   (ffirst candidate-jobs)
                                   (second (first candidate-jobs))]
                          conj (first peer-pool)))
        replica))))

(defn find-displaced-peers [replica current-allocations max-util]
  (mapcat
   (fn [job]
     (let [overflow (- (get current-allocations job) (get max-util job))]
       (when (pos? overflow)
         (cts/drop-peers replica job overflow))))
   (:jobs replica)))

(defn reconfigure-cluster-workload [replica]
  (let [job-offers (job-offer-n-peers replica)
        job-claims (job->task-claims replica job-offers)
        spare-peers (apply + (vals (merge-with - job-offers job-claims)))
        max-utilization (claim-spare-peers replica job-claims spare-peers)
        current-allocations (current-task-allocations replica)
        peers-to-displace (find-displaced-peers replica current-allocations max-utilization)]
    (reallocate-peers replica peers-to-displace max-utilization)))

(defn exempt-from-acker? [replica job task args]
  (or (some #{task} (get-in replica [:exempt-tasks job]))
      (and (get-in replica [:acker-exclude-inputs job])
           (some #{task} (get-in replica [:input-tasks job])))
      (and (get-in replica [:acker-exclude-outputs job])
           (some #{task} (get-in replica [:output-tasks job])))))

(defn offer-acker [replica job task args]
  (let [peers (count (apply concat (vals (get-in replica [:allocations job]))))
        ackers (count (get-in replica [:ackers job]))
        pct (get-in replica [:acker-percentage job])
        current-pct (int (Math/ceil (* 10 (double (/ ackers peers)))))]
    (if (and (< current-pct pct) (not (exempt-from-acker? replica job task args)))
      (-> replica
          (update-in [:ackers job] conj (:id args))
          (update-in [:ackers job] vec))
      replica)))
