(ns onyx.log.commands.complete-partition
  (:require [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.commands.common :as common]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :complete-partition
  [{:keys [args]} replica]
  (-> replica
      (assoc-in [:completed-partitions (:jobs args) (:task args) (:partition args)] (:location args))
      (reconfigure-cluster-workload)))

(defmethod extensions/replica-diff :complete-partition
  [entry old new]
  ;;; TODO: << FIX ME >>
  )

(defmethod extensions/reactions :complete-partition
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :complete-partition
  [{:keys [args]} old new diff state]
  (common/start-new-lifecycle old new diff state))
