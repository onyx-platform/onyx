(ns onyx.log.commands.signal-ready
  (:require [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :signal-ready :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [id (:id args)]
    (if (and (common/peer->allocated-job (:allocations replica) id)
             (= :idle (get-in replica [:peer-state id])))
      (assoc-in replica [:peer-state id] :active)
      replica)))

(s/defmethod extensions/replica-diff :signal-ready :- ReplicaDiff
  [{:keys [args]} :- LogEntry old new]
  (second (diff (:peer-state old) (:peer-state new))))

(s/defmethod extensions/reactions :signal-ready :- Reactions
  [{:keys [args]} :- LogEntry old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :signal-ready :- State
  [{:keys [args message-id]} :- LogEntry old new diff state]
  (let [job (:job (common/peer->allocated-job (:allocations new) (:id state)))]
    (when (common/should-seal? new {:job job} state message-id)
      (>!! (:seal-ch state) true)))
  state)
