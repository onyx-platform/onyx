(ns onyx.log.commands.set-replica
  (:require [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :set-replica! :- Replica
  [{:keys [args message-id]} :- LogEntry replica]
  (:replica args))

(s/defmethod extensions/replica-diff :set-replica!
  [entry :- LogEntry old new]
  {:diff (diff old new)})

(s/defmethod extensions/reactions :set-replica! :- Reactions
  [{:keys [args]} :- LogEntry old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :set-replica!
  [{:keys [args]} :- LogEntry old new diff state]
  state)
