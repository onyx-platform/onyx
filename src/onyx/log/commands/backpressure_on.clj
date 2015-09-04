(ns onyx.log.commands.backpressure-on
  (:require [taoensso.timbre :as timbre :refer [info error]]
            [clojure.set :refer [union]]
            [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :backpressure-on :- Replica
  [{:keys [args]} :- LogEntry replica :- Replica]
  (if (= :active (get-in replica [:peer-state (:peer args)]))
    (assoc-in replica [:peer-state (:peer args)] :backpressure)
    replica))

(s/defmethod extensions/replica-diff :backpressure-on :- ReplicaDiff
  [{:keys [args]} old new]
  (second (diff (:peer-state old) (:peer-state new))))

(s/defmethod extensions/reactions :backpressure-on :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :backpressure-on :- State
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  (if (= (:peer args) (:id state))
    (do (extensions/emit monitoring {:event :peer-backpressure-on :id (:id state)})
        state)
    state))
