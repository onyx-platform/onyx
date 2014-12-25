(ns onyx.log.commands.kill-job
  (:require [clojure.data :refer [diff]]
            [onyx.extensions :as extensions]))

(defmethod extensions/apply-log-entry :kill-job
  [{:keys [args message-id]} replica])

(defmethod extensions/replica-diff :kill-job
  [entry old new])

(defmethod extensions/fire-side-effects! :kill-job
  [{:keys [args]} old new diff state]
  state)

(defmethod extensions/reactions :kill-job
  [{:keys [args]} old new diff peer-args])

