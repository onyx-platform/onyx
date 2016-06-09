(ns onyx.log.commands.assign-bookkeeper-log-id
  (:require [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :assign-bookkeeper-log-id :- Replica
  [{:keys [args]} :- LogEntry replica]
  (update-in replica 
             [:state-logs (:job-id args) (:task-id args) (:slot-id args)]
             (fn [logs]
               (conj (vec logs) (:ledger-id args)))))

(s/defmethod extensions/replica-diff :assign-bookkeeper-log-id :- ReplicaDiff
  [{:keys [args]} :- LogEntry old new]
  (second (diff (:state-logs old) (:state-logs new))))
