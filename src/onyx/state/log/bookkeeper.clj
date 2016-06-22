(ns ^:no-doc onyx.state.log.bookkeeper
  (:require [onyx.log.curator :as curator]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer [chan timeout thread go >! <! <!! >!! alts!! close! poll!]]
            [clojure.core.async.impl.protocols :refer [closed?]]
            [onyx.compression.nippy :as nippy]
            [onyx.extensions :as extensions]
            [onyx.monitoring.measurements :refer [emit-latency-value emit-latency]]
            [onyx.peer.operation :as operation]
            [onyx.windowing.aggregation :as agg]
            [onyx.state.state-extensions :as state-extensions]
            [onyx.types :refer [inc-count! dec-count!]]
            [onyx.log.replica]
            [onyx.log.commands.common :refer [peer-slot-id]]
            [onyx.log.zookeeper :as zk]
            [onyx.static.default-vals :refer [arg-or-default defaults]])
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper BookKeeper$DigestType 
            BKException BKException$Code AsyncCallback$AddCallback]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]))

(defn event->ledger-ids [{:keys [onyx.core/replica onyx.core/job-id onyx.core/task-id] :as event}]
  (get-in @replica [:state-logs job-id task-id (peer-slot-id event)]))

(defrecord BookKeeperLog [client ledger-handle next-ledger-handle batch-ch])

(defn open-ledger ^org.apache.bookkeeper.client.LedgerHandle [^BookKeeper client id digest-type password]
  (.openLedger client id digest-type password))

(defn open-ledger-no-recovery ^org.apache.bookkeeper.client.LedgerHandle [^BookKeeper client id digest-type password]
  (.openLedgerNoRecovery client id digest-type password))

(defn create-ledger ^org.apache.bookkeeper.client.LedgerHandle [^BookKeeper client ensemble-size quorum-size digest-type password]
  (.createLedger client ensemble-size quorum-size digest-type password))

(defn close-handle [^LedgerHandle ledger-handle]
  (.close ledger-handle))

(defn ^org.apache.bookkeeper.client.BookKeeper bookkeeper
  ([opts]
   (bookkeeper (:zookeeper/address opts)
               (zk/ledgers-path (:onyx/tenancy-id opts))
               (arg-or-default :onyx.bookkeeper/client-timeout opts)
               (arg-or-default :onyx.bookkeeper/client-throttle opts)))
  ([zk-addr zk-root-path timeout throttle]
   (try
    (let [conf (doto (ClientConfiguration.)
                 (.setZkServers zk-addr)
                 (.setZkTimeout timeout)
                 (.setThrottleValue throttle)
                 (.setZkLedgersRootPath zk-root-path))]
      (BookKeeper. conf))
    (catch org.apache.zookeeper.KeeperException$NoNodeException nne
      (throw (ex-info "Error locating BookKeeper cluster via ledger path. Check that BookKeeper has been started via start-env by setting `:onyx.bookkeeper/server? true` in env-config, or is setup at the correct path." 
                      {:zookeeper-addr zk-addr
                       :zookeeper-path zk-root-path}
                      nne))))))

(def digest-type 
  (BookKeeper$DigestType/MAC))

(defn password [peer-opts]
  (.getBytes ^String (arg-or-default :onyx.bookkeeper/ledger-password peer-opts)))

(defn new-ledger ^org.apache.bookkeeper.client.LedgerHandle [client peer-opts]
  (let [ensemble-size (arg-or-default :onyx.bookkeeper/ledger-ensemble-size peer-opts)
        quorum-size (arg-or-default :onyx.bookkeeper/ledger-quorum-size peer-opts)]
    (create-ledger client ensemble-size quorum-size digest-type (password peer-opts))))

(def HandleWriteCallback
  (reify AsyncCallback$AddCallback
    (addComplete [this rc lh entry-id [success-fn fail-fn]]
      (if (= rc (BKException$Code/OK))
        (success-fn)
        (do (warn "Unable to complete async write to bookkeeper. BookKeeper exception code:" rc)
            (fail-fn))))))

(defn compaction-transition 
  "Transitions to a new compacted ledger, plus a newly created ledger created
  earlier.  For example, if there were ledgers [1, 2, 3, 4], we've created a
  ledger id 5 to start writing to, making [1, 2, 3, 4, 5], then we create a compacted
  ledger 6, write the updated state to it, and swap [1, 2, 3, 4] in the replica
  for 6, leaving [6, 5]"
  [{:keys [client ledger-handle next-ledger-handle] :as log}
   {:keys [onyx.core/peer-opts onyx.core/job-id onyx.core/replica
           onyx.core/id onyx.core/task-id onyx.core/monitoring onyx.core/window-state onyx.core/outbox-ch] 
    :as event}]
  (info "Transitioning to new handle after gc" (.getId ^LedgerHandle @next-ledger-handle))
  (let [previous-handle @ledger-handle
        start-time (System/currentTimeMillis)
        slot-id (peer-slot-id event)
        extent-snapshot (:state @window-state)
        ;; Deref future later. This way we can immediately return and continue processing
        filter-snapshot (state-extensions/snapshot-filter (:filter @window-state) event)
        current-ids (get-in @replica [:state-logs job-id task-id slot-id])]
    (reset! ledger-handle @next-ledger-handle)
    (reset! next-ledger-handle nil)
    ;; Don't throw an exception, maybe we can give the next GC a chance to succeed
    ;; Log is still in a known good state, we have transitioned to a ledger that is in the replica
    (if-not (= (last current-ids) (.getId ^LedgerHandle @ledger-handle))
      (warn "Could not swap compacted log. Next ledger handle is no longer the next published ledger" 
            {:job-id job-id :task-id task-id :slot-id slot-id 
             :ledger-handle (.getId ^LedgerHandle @ledger-handle) :current-ids current-ids})
      (future 
        (let [;; Write compacted as a batch
              compacted [{:type :compacted
                          ;; TODO: add a timeout when derefing here.
                          :filter-snapshot @filter-snapshot
                          :extent-state extent-snapshot}]
              compacted-ledger (new-ledger client peer-opts)
              compacted-ledger-id (.getId compacted-ledger)
              compacted-serialized ^bytes (nippy/window-log-compress compacted)]
          (.asyncAddEntry compacted-ledger 
                          compacted-serialized
                          HandleWriteCallback
                          (list (fn []
                                  (close-handle previous-handle)
                                  (emit-latency-value :window-log-compaction monitoring (- (System/currentTimeMillis) start-time))
                                  (>!! outbox-ch
                                       {:fn :compact-bookkeeper-log-ids
                                        :args {:job-id job-id
                                               :task-id task-id
                                               :slot-id slot-id
                                               :peer-id id
                                               :prev-ledger-ids (vec (butlast current-ids))
                                               :new-ledger-ids [compacted-ledger-id]}}))
                                (fn [] 
                                  (>!! (:onyx.core/group-ch event) [:restart-vpeer (:onyx.core/id event)])))))))))

(defn assign-bookkeeper-log-id-spin [{:keys [onyx.core/peer-opts
                                             onyx.core/job-id onyx.core/task-id
                                             onyx.core/kill-ch onyx.core/task-kill-ch
                                             onyx.core/outbox-ch] :as event}
                                     new-ledger-id]
  (let [slot-id (peer-slot-id event)]
    (>!! outbox-ch
         {:fn :assign-bookkeeper-log-id
          :args {:job-id job-id
                 :task-id task-id
                 :slot-id slot-id
                 :ledger-id new-ledger-id}})
    (loop []
      (let [exit? (nil? (first (alts!! [kill-ch task-kill-ch] :default true)))
            not-added? (not= new-ledger-id (last (event->ledger-ids event)))]
        (cond exit? 
              (info "Exiting assign-bookkeeper-log-id-spin early as peer has been reassigned.")
              not-added? 
              (do
                (info "New ledger id has not been published yet. Backing off.")
                (Thread/sleep (arg-or-default :onyx.bookkeeper/ledger-id-written-back-off peer-opts))
                (recur))))))) 

(defmethod state-extensions/initialize-log :bookkeeper [log-type {:keys [onyx.core/peer-opts] :as event}] 
  (let [bk-client (bookkeeper peer-opts)
        ledger-handle (new-ledger bk-client peer-opts)
        new-ledger-id (.getId ledger-handle)
        batch-ch (chan (arg-or-default :onyx.bookkeeper/write-buffer-size peer-opts))
        next-ledger-handle nil] 
    (assign-bookkeeper-log-id-spin event new-ledger-id)
    (info "Ledger id" new-ledger-id "published")
    (->BookKeeperLog bk-client (atom ledger-handle) (atom next-ledger-handle) batch-ch)))

(defn playback-batch-entry [state apply-entry-fn batch]
  (reduce apply-entry-fn state batch))

(defn playback-entries-chunk [state apply-entry-fn ^LedgerHandle lh start end event]
  (let [entries (.readEntries lh start end)]
    (if (.hasMoreElements entries)
      (loop [state' state element ^LedgerEntry (.nextElement entries)]
        (let [entry-val (nippy/window-log-decompress ^bytes (.getEntry element))
              state'' (playback-batch-entry state' apply-entry-fn entry-val)]
          (if (.hasMoreElements entries)
            (recur state'' (.nextElement entries))
            state'')))
      state)))

(defn check-abort-playback! 
  "Check whether playback should be aborted if the peer is already rescheduled or killed"
  [{:keys [onyx.core/task-kill-ch onyx.core/kill-ch] :as event}]
  (when (nil? (first (alts!! [kill-ch task-kill-ch] :default true)))
    (throw (ex-info "Playback aborted as peer has been rescheduled during state-log playback. Restarting peer."
                    {:playback-aborted? true}))))

(defn playback-ledger [state apply-entry-fn ^LedgerHandle lh last-confirmed {:keys [onyx.core/peer-opts] :as event}]
  (let [chunk-size (arg-or-default :onyx.bookkeeper/read-batch-size peer-opts)]
    (if-not (neg? last-confirmed)
      (loop [loop-state state start 0 end (min chunk-size last-confirmed)] 
        (check-abort-playback! event)
        (let [new-state (playback-entries-chunk loop-state apply-entry-fn lh start end event)]
          (if (= end last-confirmed)
            new-state
            (recur new-state 
                   (inc end) 
                   (min (+ chunk-size end) last-confirmed)))))
      state)))

(defn playback-ledgers [bk-client state apply-entry-fn ledger-ids {:keys [onyx.core/peer-opts] :as event}]
  (try 
    (let [pwd (password peer-opts)]
      (reduce (fn [state' ledger-id]
                (let [lh (open-ledger bk-client ledger-id digest-type pwd)]
                  (try
                    (let [last-confirmed (.getLastAddConfirmed lh)]
                      (info "Opened ledger:" ledger-id "last confirmed:" last-confirmed)
                      (playback-ledger state' apply-entry-fn lh last-confirmed event))
                    (finally
                      (close-handle lh)))))
              state
              ledger-ids))
    (catch clojure.lang.ExceptionInfo e
      ;; Playback was aborted, safe to return empty state
      ;; as peer will no longer be allocated to this task
      (if (:playback-aborted? (ex-data e))
        (do
          (warn "Playback aborted as task or peer was killed." (ex-data e))
          state)
        (throw e)))))

(defmethod state-extensions/playback-log-entries onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [client] :as log} 
   {:keys [onyx.core/monitoring onyx.core/task-id] :as event} 
   state
   apply-entry-fn]
  (emit-latency :window-log-playback 
                monitoring
                (fn [] 
                    (let [;; Don't play back the final ledger id because we just created it
                          prev-ledger-ids (butlast (event->ledger-ids event))]
                      (info "Playing back ledgers for" task-id "ledger-ids" prev-ledger-ids)
                      (playback-ledgers client state apply-entry-fn prev-ledger-ids event)))))

(defmethod state-extensions/compact-log onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [client ledger-handle next-ledger-handle]} 
   {:keys [onyx.core/peer-opts] :as event} 
   _] 
  (future
    (let [new-ledger-handle (new-ledger client peer-opts)
          new-ledger-id (.getId new-ledger-handle)] 
      (assign-bookkeeper-log-id-spin event new-ledger-id)
      (reset! next-ledger-handle new-ledger-handle))))

(defmethod state-extensions/close-log onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [client ledger-handle next-ledger-handle]} event] 
  (try
    (when @ledger-handle 
      (close-handle @ledger-handle))
    (when @next-ledger-handle
      (close-handle @next-ledger-handle))
    (catch Throwable t
      (warn t "Error closing BookKeeper handle")))
  (.close ^BookKeeper client))

(defmethod state-extensions/store-log-entry onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [ledger-handle next-ledger-handle batch-ch] :as log} event ack-fn entry]
  (.asyncAddEntry ^LedgerHandle @ledger-handle 
                  ^bytes (nippy/window-log-compress entry)
                  HandleWriteCallback
                  (list ack-fn
                        (fn [] 
                          (>!! (:onyx.core/group-ch event) [:restart-vpeer (:onyx.core/id event)])))))
