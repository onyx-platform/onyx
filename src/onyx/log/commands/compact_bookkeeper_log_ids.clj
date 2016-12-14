(ns onyx.log.commands.compact-bookkeeper-log-ids
  (:require [clojure.core.async :refer [>!!]]
            [clojure.data :refer [diff]]
            [onyx.log.commands.common :as common]
            [onyx.log.entry :refer [create-log-entry]]
            [schema.core :as s]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx.schema :refer [Replica LogEntry Reactions ReplicaDiff State]]
            ;[onyx.state.log.bookkeeper :as logbk]
            [onyx.extensions :as extensions])
  (:import [org.apache.bookkeeper.client BKException$BKNoSuchLedgerExistsException]))

; (s/defmethod extensions/apply-log-entry :compact-bookkeeper-log-ids :- Replica
;   [{:keys [args]} :- LogEntry replica]
;   (let [prev-ledger-ids (:prev-ledger-ids args)
;         new-ledger-ids (:new-ledger-ids args)
;         new (update-in replica 
;                        [:state-logs (:job-id args) (:task-id args) (:slot-id args)]
;                        (fn [logs]
;                          (let [n-prev (count prev-ledger-ids)
;                                swapable? (= (take n-prev logs) 
;                                             prev-ledger-ids)] 
;                            (if swapable? 
;                              (into (vec new-ledger-ids) 
;                                    (drop n-prev logs))
;                              ;; Replica state is not what was expected, leave alone
;                              logs))))]
;     ;; If nothing changed, delete the new compacted ledger
;     ;; Otherwise, delete the stale ledgers that have been compacted
;     (update-in new [:state-logs-marked] into (if (= new replica)
;                                                (:new-ledger-ids args)
;                                                (:prev-ledger-ids args)))))

; (s/defmethod extensions/replica-diff :compact-bookkeeper-log-ids :- ReplicaDiff
;   [{:keys [args]} :- LogEntry old new]
;   (second (diff (:state-logs old) (:state-logs new))))

; (s/defmethod extensions/fire-side-effects! [:compact-bookkeeper-log-ids :peer] :- State
;   [{:keys [args message-id]} :- LogEntry old new diff state]
;   ;; FIXME: should be the peer assigned to the slot id that 
;   ;; deletes the ledgers not the peer that submitted the swap
;   (when (= (:id state) (:peer-id args))
;     ;; If nothing changed, delete the new compacted ledger
;     ;; Otherwise, delete the stale ledgers that have been compacted
;     (let [delete-ids (if (= new old)
;                        (:new-ledger-ids args)
;                        (:prev-ledger-ids args))
;           bk-client (logbk/bookkeeper (:opts state))]
;       (doseq [ledger-id delete-ids]
;         (info "Deleting compacted ledger id:" ledger-id)
;         (try (.deleteLedger bk-client ledger-id)
;              (catch BKException$BKNoSuchLedgerExistsException e
;                (warn "Deleting ledger that no longer exists." e))))
;       (>!! (:outbox-ch state)
;            {:fn :deleted-bookkeeper-log-ids
;             :args {:logs delete-ids}
;             :peer-parent (:id state)
;             :entry-parent message-id})
;       (.close bk-client)))
;   state)
