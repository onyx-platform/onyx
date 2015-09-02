(ns onyx.log.commands.backpressure-off
  (:require [taoensso.timbre :as timbre :refer [info error]]
            [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [schema.core :as s]
            [onyx.schema :refer [Replica LogEntry Reactions]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :backpressure-off :- Replica
  [{:keys [args]} :- LogEntry replica]
  (if (= :backpressure (get-in replica [:peer-state (:peer args)]))
    (assoc-in replica [:peer-state (:peer args)] :active)
    replica))

(defmethod extensions/replica-diff :backpressure-off
  [{:keys [args]} old new]
  (second (diff (:peer-state old) (:peer-state new))))

(s/defmethod extensions/reactions :backpressure-off :- Reactions
  [{:keys [args]} old new diff peer-args]
  [])

(defmethod extensions/fire-side-effects! :backpressure-off
  [{:keys [args]} old new diff {:keys [monitoring] :as state}]
  (if (= (:peer args) (:id state))
    (do (extensions/emit monitoring {:event :peer-backpressure-off :id (:id state)})
        state)
    state))
