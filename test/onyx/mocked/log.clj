(ns onyx.mocked.log
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.log.replica]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.extensions :as extensions]))

; (defrecord MockedLog [store job-id task-id slot-id])

; (defn new-mocked-log [job-id task-id slot-id]
;   (->MockedLog (atom {}) job-id task-id slot-id))

; (defmethod state-extensions/initialize-log :mocked-log 
;   [log-type {:keys [job-id task-id slot-id] :as event}] 
;   (new-mocked-log job-id task-id slot-id))

; (defn playback-batch-entry [state apply-entry-fn batch]
;   (reduce apply-entry-fn state batch))

; (defn playback-entries-chunk [state apply-entry-fn ^LedgerHandle lh start end event]
;   (let [entries (.readEntries lh start end)]
;     (if (.hasMoreElements entries)
;       (loop [state' state element ^LedgerEntry (.nextElement entries)]
;         (let [entry-val (nippy/window-log-decompress ^bytes (.getEntry element))
;               state'' (playback-batch-entry state' apply-entry-fn entry-val)]
;           (if (.hasMoreElements entries)
;             (recur state'' (.nextElement entries))
;             state'')))
;       state)))

; (defn playback-ledger [state apply-entry-fn ^LedgerHandle lh last-confirmed {:keys [peer-opts] :as event}]
;   (let [chunk-size (arg-or-default :onyx.bookkeeper/read-batch-size peer-opts)]
;     (if-not (neg? last-confirmed)
;       (loop [loop-state state start 0 end (min chunk-size last-confirmed)] 
;         (check-abort-playback! event)
;         (let [new-state (playback-entries-chunk loop-state apply-entry-fn lh start end event)]
;           (if (= end last-confirmed)
;             new-state
;             (recur new-state 
;                    (inc end) 
;                    (min (+ chunk-size end) last-confirmed)))))
;       state)))

; (defn playback-ledgers [bk-client state apply-entry-fn ledger-ids {:keys [peer-opts] :as event}]
;   (try 
;     (let [pwd (password peer-opts)]
;       (reduce (fn [state' ledger-id]
;                 (let [lh (open-ledger bk-client ledger-id digest-type pwd)]
;                   (try
;                     (let [last-confirmed (.getLastAddConfirmed lh)]
;                       (info "Opened ledger:" ledger-id "last confirmed:" last-confirmed)
;                       (playback-ledger state' apply-entry-fn lh last-confirmed event))
;                     (finally
;                       (close-handle lh)))))
;               state
;               ledger-ids))
;     (catch clojure.lang.ExceptionInfo e
;       ;; Playback was aborted, safe to return empty state
;       ;; as peer will no longer be allocated to this task
;       (if (:playback-aborted? (ex-data e))
;         (do
;           (warn "Playback aborted as task or peer was killed." (ex-data e))
;           state)
;         (throw e)))))

; (defmethod state-extensions/playback-log-entries onyx.state.log.bookkeeper.BookKeeperLog
;   [{:keys [client] :as log} 
;    {:keys [monitoring task-id] :as event} 
;    state
;    apply-entry-fn]
;   ; (emit-latency :window-log-playback 
;   ;               monitoring
;   ;               (fn [] 
;   ;                   (let [;; Don't play back the final ledger id because we just created it
;   ;                         prev-ledger-ids (butlast (event->ledger-ids event))]
;   ;                     (info "Playing back ledgers for" task-id "ledger-ids" prev-ledger-ids)
;   ;                     (playback-ledgers client state apply-entry-fn prev-ledger-ids event))))
;   event
;   )

; (defmethod state-extensions/compact-log onyx.state.log.bookkeeper.BookKeeperLog
;   [{:keys [] :as log} 
;    {:keys [peer-opts] :as event} 
;    _] 
;   )

; (defmethod state-extensions/close-log onyx.state.log.bookkeeper.BookKeeperLog
;   [{:keys [] :as log} event] 
;   )

; (defmethod state-extensions/store-log-entry onyx.state.log.bookkeeper.BookKeeperLog
;   [log event ack-fn entry]
;   (println "entry " entry)
;   )
