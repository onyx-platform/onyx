(ns onyx.log.commands.update-peer-state
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :update-peer-state
  [{:keys [args]} replica]
  (-> replica
      (assoc-in [:peer-state (:peer args)] (:state args))))

(defmethod extensions/replica-diff :update-peer-state
  [{:keys [args]} old new]
  args)

(defmethod extensions/fire-side-effects! :update-peer-state
  [{:keys [args]} old new diff state]
  state)

(defmethod extensions/reactions :update-peer-state
  [{:keys [args]} old new diff peer-args]
  [])

