(ns onyx.log.commands.broadcast-input-partitions
  (:require [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :broadcast-input-partitions
  [{:keys [args]} replica]
  (let [tasks (get-in replica [:tasks (:job args)])
        partitions (zipmap tasks (repeat (:n-partitions args)))]
    (-> replica
        (assoc-in [:partitions (:job args)] partitions)
        (reconfigure-cluster-workload))))

(defmethod extensions/replica-diff :broadcast-input-partitions
  [entry old new]
  ;;; TODO: << FIX ME >>
  )

(defmethod extensions/reactions :broadcast-input-partitions
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :broadcast-input-partitions
  [{:keys [args]} old new diff state]
  (common/start-new-lifecycle old new diff state))
