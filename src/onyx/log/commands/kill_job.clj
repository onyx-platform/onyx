(ns onyx.log.commands.kill-job
  (:require [clojure.data :refer [diff]]
            [com.stuartsierra.component :as component]
            [onyx.log.commands.common :refer [peer->allocated-job] :as common]
            [onyx.scheduling.common-job-scheduler :as cjs]
            [onyx.extensions :as extensions]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]))

(defmethod extensions/apply-log-entry :kill-job
  [{:keys [args]} replica]
  (if-not (some #{(:job args)} (:killed-jobs replica))
    (let [peers (mapcat identity (vals (get-in replica [:allocations (:job args)])))]
      (-> replica
          (update-in [:jobs] (fn [coll] (remove (partial = (:job args)) coll)))
          (update-in [:jobs] vec)
          (update-in [:killed-jobs] conj (:job args))
          (update-in [:killed-jobs] vec)
          (update-in [:allocations] dissoc (:job args))
          (update-in [:ackers] dissoc (:job args))
          (merge {:peer-state (into {} (map (fn [p] {p :idle}) peers))})
          (reconfigure-cluster-workload)))
    replica))

(defmethod extensions/replica-diff :kill-job
  [entry old new]
  (second (diff (into #{} (:killed-jobs old)) (into #{} (:killed-jobs new)))))

(defn executing-killed-job? [diff replica job-id peer-id]
  (and diff (= (:job (peer->allocated-job (:allocations replica) peer-id)) job-id)))

(defmethod extensions/reactions :kill-job
  [{:keys [args]} old new diff state]
  [])

(defmethod extensions/fire-side-effects! :kill-job
  [{:keys [args]} old new diff state]
  (common/start-new-lifecycle old new diff state))
