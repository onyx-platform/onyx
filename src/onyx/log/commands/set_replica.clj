(ns onyx.log.commands.set-replica
  (:require [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :set-replica! :- Replica
  [{:keys [args message-id]} :- LogEntry replica]
  (:replica args))

(s/defmethod extensions/replica-diff :set-replica! :- ReplicaDiff
  [entry :- LogEntry old new]
  {:diff (diff old new)})

(s/defmethod extensions/reactions :set-replica! :- Reactions
  [{:keys [args]} :- LogEntry old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :set-replica! :- State
  [{:keys [args]} :- LogEntry old new diff state]
  state)
