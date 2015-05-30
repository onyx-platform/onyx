(ns onyx.log.commands.set-replica
  (:require [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :set-replica!
  [{:keys [args message-id]} replica]
  (:replica args))

(defmethod extensions/replica-diff :set-replica!
  [entry old new]
  {:diff (diff old new)})

(defmethod extensions/reactions :set-replica!
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :set-replica!
  [{:keys [args]} old new diff state]
  state)
