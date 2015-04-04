(ns onyx.log.commands.kill-job
  (:require [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :refer [incomplete-jobs peer->allocated-job]]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :kill-job
  [{:keys [args]} replica]
  (let [peers (mapcat identity (vals (get-in replica [:allocations (:job args)])))]
    (if (some #{(:job args)} (into #{} (incomplete-jobs replica)))
      (-> replica
          ;; Scheduler TODO: move job out of :jobs to :killed-jobs
          (update-in [:killed-jobs] conj (:job args))
          (update-in [:killed-jobs] vec)
          (update-in [:allocations] dissoc (:job args))
          (merge {:peer-state (into {} (map (fn [p] {p :idle}) peers))}))
      replica)))

(defmethod extensions/replica-diff :kill-job
  [entry old new]
  (second (diff (into #{} (:killed-jobs old)) (into #{} (:killed-jobs new)))))

(defn executing-killed-job? [diff replica job-id peer-id]
  (and diff (= (:job (peer->allocated-job (:allocations replica) peer-id)) job-id)))

(defmethod extensions/reactions :kill-job
  [{:keys [args]} old new diff state]
  (when (and (executing-killed-job? diff old (:job args) (:id state))
             (cjs/volunteer-via-killed-job? old new diff state))
    (do ;; SCHEDULER TODO: << Removed volunteer >>
      nil)))

(defmethod extensions/fire-side-effects! :kill-job
  [{:keys [args]} old new diff state]
  (if (executing-killed-job? diff old (:job args) (:id state))
    (do (component/stop @(:lifecycle state))
        (assoc state :lifecycle nil))
    state))

