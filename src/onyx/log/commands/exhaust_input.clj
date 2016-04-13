(ns onyx.log.commands.exhaust-input
  (:require [clojure.core.async :refer [>!!]]
            [clojure.set :refer [union]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]
            [onyx.log.commands.common :as common]))

(s/defmethod extensions/apply-log-entry :exhaust-input :- Replica
  [{:keys [args]} :- LogEntry replica]
  (if (some #{(:job args)} (:jobs replica)) 
    (update-in replica [:exhausted-inputs (:job args)] union #{(:task args)})
    replica))

(s/defmethod extensions/replica-diff :exhaust-input :- ReplicaDiff
  [{:keys [args]} old new]
  {:job (:job args) :task (:task args)})

(s/defmethod extensions/reactions :exhaust-input :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :exhaust-input :- State
  [{:keys [args message-id]} old new diff state]
  (when (common/should-seal? new (:job args) state message-id)
    (>!! (:seal-ch (:task-state state)) true))
  state)
