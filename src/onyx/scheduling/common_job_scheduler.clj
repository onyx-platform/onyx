(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]
            [onyx.extensions :as extensions]
            [taoensso.timbre]))

(defn balance-workload [replica jobs p]
  (if (seq jobs)
    (let [j (count jobs)
          min-peers (int (/ p j))
          n (rem p j)
          max-peers (inc min-peers)]
      (map-indexed
       (fn [i job]
         [job (if (< i n) max-peers min-peers)])
       jobs))
    []))

(defn unbounded-jobs [replica balanced]
  (filter
   (fn [[job _]]
     (= (get-in replica [:saturation job] Double/POSITIVE_INFINITY)
        Double/POSITIVE_INFINITY))
   balanced))

(defn adjust-with-overflow [replica balanced]
  (reduce
   (fn [result [job n]]
     (let [sat (or (get-in replica [:saturation job]) Double/POSITIVE_INFINITY)
           extra (- n sat)]
       (if (pos? extra)
         (-> result
             (update-in [:overflow] + extra)
             (update-in [:jobs] conj [job sat]))
         (-> result
             (update-in [:jobs] conj [job n])))))
   {:overflow 0 :jobs []}
   balanced))

(defn balance-jobs [replica]
  (let [balanced (balance-workload replica (:jobs replica) (count (:peers replica)))
        {:keys [overflow jobs]} (adjust-with-overflow replica balanced)
        unbounded (unbounded-jobs replica jobs)]
    (merge-with
     +
     (into {} (balance-workload replica (map first unbounded) overflow))
     (into {} jobs))))

(defn incomplete-jobs [replica]
  (filter
   #(< (count (get-in replica [:completions %]))
       (count (get-in replica [:tasks %])))
   (:jobs replica)))

(defn jobs-with-available-tasks [replica jobs]
  (filter
   (fn [job]
     (let [tasks (get-in replica [:tasks job])]
       (seq (active-tasks-only replica tasks))))
   jobs))

(defn alive-jobs [replica jobs]
  (let [dead-jobs (into #{} (:killed-jobs replica))]
    (remove (fn [job] (some #{job} dead-jobs)) jobs)))

(defn job-coverable? [replica job]
  (let [tasks (get-in replica [:tasks job])]
    (>= (count (get-in replica [:peers])) (count tasks))))

(defn at-least-one-active? [replica peers]
  (->> peers
       (map #(get-in replica [:peer-state %]))
       (filter (partial = :active))
       (seq)))

(defn job-covered? [replica job]
  (let [tasks (get-in replica [:tasks job])
        active? (partial at-least-one-active? replica)]
    (every? identity (map #(active? (get-in replica [:allocations job %])) tasks))))

(defmulti select-job
  (fn [{:keys [args]} replica]
    (:job-scheduler replica)))

(defn universally-executable-jobs [replica]
  (->> replica
       (common/incomplete-jobs)
       (common/alive-jobs replica)
       (common/jobs-with-available-tasks replica)))

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

(defmethod select-job :default
  [_ replica]
  (throw (ex-info 
           (format "Job scheduler %s not recognized. Check that you have not supplied a task scheduler instead." 
                   (:job-scheduler replica))
           {:replica replica})))

(defmulti volunteer-via-new-job?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-killed-job?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-sealed-output?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-accept?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti volunteer-via-leave?
  (fn [old new diff state]
    (:job-scheduler old)))

(defmulti reallocate-from-job?
  (fn [scheduler old new state]
    scheduler))
