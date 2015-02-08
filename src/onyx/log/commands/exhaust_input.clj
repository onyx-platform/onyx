(ns onyx.log.commands.exhaust-input
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]))

(defmethod extensions/apply-log-entry :exhaust-input
  [{:keys [args]} replica]
  (update-in replica [:exhausted-inputs (:job args)] union #{(:task args)}))

(defmethod extensions/replica-diff :exhaust-input
  [{:keys [args]} old new]
  {:job (:job args) :task (:task args)})

(defmethod extensions/reactions :exhaust-input
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :exhaust-input
  [{:keys [args message-id]} old new diff state]
  (when (and (common/all-inputs-exhausted? new (:job args))
             (common/executing-output-task? new (:job args) (:task args))
             (common/elected-sealer? new message-id (:job args) (:task args) (:id state)))
    (>!! (:seal-response-ch state) true))
  state)

