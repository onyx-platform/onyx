(ns onyx.state.log.bookkeeper
  (:require [onyx.log.curator :as curator]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer  [chan go >! <! <!! >!! alts!! close!]]
            [taoensso.nippy :as nippy]
            [onyx.extensions :as extensions]
            [onyx.state.core :as state]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.log.replica]
            [onyx.log.commands.assign-bookkeeper-log-id]
            [onyx.log.zookeeper :as zk]
            [onyx.static.default-vals :refer [arg-or-default defaults]])
  (:import [org.apache.bookkeeper.client LedgerHandle BookKeeper BookKeeper$DigestType]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]))

(defn open-ledger ^LedgerHandle [client id digest-type password]
  (.openLedger client id digest-type password))

(defn create-ledger ^LedgerHandle [client ensemble-size quorum-size digest-type password]
  (.createLedger client ensemble-size quorum-size digest-type password))

(defn bookkeeper
  ([opts]
   (bookkeeper (:zookeeper/address opts)
               (zk/ledgers-path (:onyx/id opts))
               (arg-or-default :onyx.bookkeeper/timeout opts)))
  ([zk-addr zk-root-path timeout]
   (let [conf (doto (ClientConfiguration.)
                (.setZkServers zk-addr)
                (.setZkTimeout timeout)
                (.setZkLedgersRootPath zk-root-path))]
     (BookKeeper. conf))))

(def digest-type 
  (BookKeeper$DigestType/MAC))

(defn password [peer-opts]
  (.getBytes (arg-or-default :onyx.bookkeeper/ledger-password peer-opts)))

;; TODO: add zk-timeout for bookkeeper
(defmethod state-extensions/initialise-log :bookkeeper [log-type {:keys [onyx.core/replica onyx.core/peer-opts
                                                                         onyx.core/job-id onyx.core/task-id
                                                                         onyx.core/kill-ch onyx.core/task-kill-ch
                                                                         onyx.core/outbox-ch] :as event}] 
  (let [bk-client (bookkeeper peer-opts)
        ensemble-size (arg-or-default :onyx.bookkeeper/ledger-ensemble-size peer-opts)
        quorum-size (arg-or-default :onyx.bookkeeper/ledger-quorum-size peer-opts)
        ledger (create-ledger bk-client ensemble-size quorum-size digest-type (password peer-opts))
        slot-id (state/peer-slot-id event)
        new-ledger-id (.getId ledger)] 
    (>!! outbox-ch
         {:fn :assign-bookkeeper-log-id
          :args {:job-id job-id
                 :task-id task-id
                 :slot-id slot-id
                 :ledger-id new-ledger-id}})
    (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                (not= new-ledger-id
                      (last (get-in @replica [:state-logs job-id task-id slot-id]))))
      (info "New ledger id has not been published yet. Backing off.")
      (Thread/sleep (arg-or-default :onyx.bookkeeper/ledger-id-written-back-off peer-opts))) 
    (info "Ledger id published.")
    ledger))


(defn default-state 
  "Default state function. Resolves window late for perf."
  [state windows window-id]
  (if state
    state
    (let [w (first (filter #(= window-id (:window/id %)) windows))] 
      ((:window/agg-init w) w))))

(defn playback-ledgers [bk-client peer-opts state ledger-ids windows]
  (let [pwd (password peer-opts)
        id->log-resolve (into {} 
                              (map (juxt :window/id :window/log-resolve) 
                                   windows))] 
    (reduce (fn [st ledger-id]
              ;; TODO: Do I need to deal with recovery exception in here?
              ;; It may be better to just let the thing crash and retry
              (let [lh (open-ledger bk-client ledger-id digest-type pwd)]
                (try
                  (let [last-confirmed (.getLastAddConfirmed lh)
                        _ (info "Opened ledger:" ledger-id "last confirmed:" last-confirmed)]
                    (if (pos? last-confirmed)
                      (let [entries (.readEntries lh 0 last-confirmed)] 
                        (if (.hasMoreElements entries)
                          (loop [st-loop st element (.nextElement entries)]
                            (let [[window-id extent entry] (nippy/thaw (.getEntry element))
                                  _ (info "Read entry" window-id extent entry)
                                  st-loop' (update-in st-loop 
                                                      [window-id extent]
                                                      (fn [ext-state] 
                                                        (let [ext-state' (default-state ext-state windows window-id)
                                                              apply-fn (id->log-resolve window-id)] 
                                                          (assert apply-fn (str "Apply fn does not exist for window-id " window-id))
                                                          (apply-fn ext-state' entry))))]
                              (if (.hasMoreElements entries) 
                                (recur st-loop' (.nextElement entries))
                                st-loop')))
                          st))  
                      st))
                  (finally
                    (.close lh)))))
            state
            ledger-ids))) 

(defmethod state-extensions/playback-log-entries org.apache.bookkeeper.client.LedgerHandle 
  [log {:keys [onyx.core/windows onyx.core/replica onyx.core/peer-opts onyx.core/job-id onyx.core/task-id] :as event} state]
  (let [slot-id (state/peer-slot-id event)
        ;; Don't play back the final ledger id because we just created it
        previous-ledger-ids (butlast (get-in @replica [:state-logs job-id task-id slot-id]))
        bk-client (bookkeeper peer-opts)
        _ (info "Playing back ledgers for" job-id task-id slot-id "ledger-ids" previous-ledger-ids)]
    (try
      (playback-ledgers bk-client peer-opts state previous-ledger-ids windows)
      (finally
        (.close bk-client)))))

(defmethod state-extensions/close-log org.apache.bookkeeper.client.LedgerHandle
  [log event] 
  (.close ^LedgerHandle log))

(defmethod state-extensions/store-log-entry org.apache.bookkeeper.client.LedgerHandle 
  [log event entry]
  (let [start-time (System/currentTimeMillis)]
    (info "Writing entry " entry)
    ;; TODO: make add entry async and use acking
    (.addEntry ^LedgerHandle log (nippy/freeze entry {}))
    (info "TOOK: " (- (System/currentTimeMillis) start-time)))
  ;; TODO: write out batch end (don't apply until batch end found in others)
  )
