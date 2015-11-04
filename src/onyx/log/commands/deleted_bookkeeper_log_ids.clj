(ns onyx.log.commands.deleted-bookkeeper-log-ids
  (:require [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [schema.core :as s]
            [clojure.set :refer [difference]]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :deleted-bookkeeper-log-ids :- Replica
  [{:keys [args]} :- LogEntry replica]
  (update replica :state-logs-marked difference (:logs args)))

(s/defmethod extensions/replica-diff :deleted-bookkeeper-log-ids :- ReplicaDiff
  [{:keys [args]} :- LogEntry old new]
  (second (diff (:state-logs-marked old) (:state-logs-marked new))))

(s/defmethod extensions/reactions :deleted-bookkeeper-log-ids :- Reactions
  [{:keys [args]} :- LogEntry old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :deleted-bookkeeper-log-ids :- State
  [{:keys [args message-id]} :- LogEntry old new diff state]
  state)
