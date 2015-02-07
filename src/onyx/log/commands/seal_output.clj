(ns onyx.log.commands.seal-output
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :seal-output
  [{:keys [args]} replica])

(defmethod extensions/replica-diff :seal-output
  [entry old new]
  {})

(defmethod extensions/fire-side-effects! :seal-output
  [{:keys [args]} old new diff state]
  state)

(defmethod extensions/reactions :seal-output
  [{:keys [args]} old new diff peer-args]
  [])

