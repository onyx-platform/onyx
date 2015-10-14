(ns onyx.log.commands.compact-bookkeeper-log-ids
  (:require [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [schema.core :as s]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            [onyx.extensions :as extensions]))

(s/defmethod extensions/apply-log-entry :compact-bookkeeper-log-ids :- Replica
  [{:keys [args]} :- LogEntry replica]
  (let [prev-ledger-ids (:prev-ledger-ids args)
        new-ledger-ids (:new-ledger-ids args)]
    (update-in replica 
               [:state-logs (:job-id args) (:task-id args) (:slot-id args)]
               (fn [logs]
                 (let [n-prev (count prev-ledger-ids)
                       swapable? (= (take n-prev logs) 
                                     prev-ledger-ids)] 
                   (if swapable? 
                     (into (vec new-ledger-ids) 
                           (drop n-prev logs))
                     ;; Replica state is not what was expected, leave alone
                     logs))))))

(s/defmethod extensions/replica-diff :compact-bookkeeper-log-ids :- ReplicaDiff
  [{:keys [args]} :- LogEntry old new]
  (second (diff (:state-logs old) (:state-logs new))))

(s/defmethod extensions/reactions :compact-bookkeeper-log-ids :- Reactions
  [{:keys [args]} :- LogEntry old new diff peer-args]
  [])

(s/defmethod extensions/fire-side-effects! :compact-bookkeeper-log-ids :- State
  [{:keys [args message-id]} :- LogEntry old new diff state]
  ;;; PXIME, clean up delete swapped ledgers
  state)
