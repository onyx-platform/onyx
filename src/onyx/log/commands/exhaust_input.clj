(ns onyx.log.commands.exhaust-input
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions]]
            [onyx.extensions :as extensions]
            [onyx.log.commands.common :as common]))

(s/defmethod extensions/apply-log-entry :exhaust-input :- Replica
  [{:keys [args]} :- LogEntry replica]
  (update-in replica [:exhausted-inputs (:job args)] union #{(:task args)}))

(s/defmethod extensions/replica-diff :exhaust-input
  [{:keys [args]} old new]
  {:job (:job args) :task (:task args)})

(s/defmethod extensions/reactions :exhaust-input :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :exhaust-input
  [{:keys [args message-id]} old new diff state]
  (when (common/should-seal? new args state message-id)
    (>!! (:seal-ch state) true))
  state)
