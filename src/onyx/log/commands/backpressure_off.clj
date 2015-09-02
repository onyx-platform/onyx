(ns onyx.log.commands.backpressure-off
  (:require [taoensso.timbre :as timbre :refer [info error]]
            [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :backpressure-off :- Replica
  [{:keys [args]} :- LogEntry replica]
  (if (= :backpressure (get-in replica [:peer-state (:peer args)]))
    (assoc-in replica [:peer-state (:peer args)] :active)
    replica))

(s/defmethod extensions/replica-diff :backpressure-off :- ReplicaDiff
  [{:keys [args]} old new]
  (second (diff (:peer-state old) (:peer-state new))))

(s/defmethod extensions/reactions :backpressure-off :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :backpressure-off :- State
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  (if (= (:peer args) (:id state))
    (do (extensions/emit monitoring {:event :peer-backpressure-off :id (:id state)})
        state)
    state))
