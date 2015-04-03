(ns onyx.scheduling.common-job-scheduler
  (:require [clojure.core.async :refer [chan go >! <! close! >!!]]
            [clojure.set :refer [union difference map-invert]]
            [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :as common]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]
            [onyx.extensions :as extensions]
            [taoensso.timbre]))

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
