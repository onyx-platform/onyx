(ns onyx.log.commands.exhaust-input
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :exhaust-input
  [{:keys [args]} replica])

(defmethod extensions/replica-diff :exhaust-input
  [entry old new]
  {})

(defmethod extensions/fire-side-effects! :exhaust-input
  [{:keys [args]} old new diff state]
  state)

(defmethod extensions/reactions :exhaust-input
  [{:keys [args]} old new diff peer-args]
  [])

