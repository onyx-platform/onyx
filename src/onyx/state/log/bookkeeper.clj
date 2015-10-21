(ns ^:no-doc onyx.state.log.bookkeeper
  (:require [onyx.log.curator :as curator]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.core.async :refer  [chan timeout thread go >! <! <!! >!! alts!! close!]]
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
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper BookKeeper$DigestType AsyncCallback$AddCallback]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]))

(defrecord BookKeeperLog [client ledger-handle next-ledger-handle batch-ch])

(defn open-ledger ^LedgerHandle [^BookKeeper client id digest-type password]
  (.openLedger client id digest-type password))

(defn create-ledger ^LedgerHandle [^BookKeeper client ensemble-size quorum-size digest-type password]
  (.createLedger client ensemble-size quorum-size digest-type password))

(defn close-handle [^LedgerHandle ledger-handle]
  (.close ledger-handle))

(defn bookkeeper
  ([opts]
   (bookkeeper (:zookeeper/address opts)
               (zk/ledgers-path (:onyx/id opts))
               (arg-or-default :onyx.bookkeeper/client-timeout opts)
               (arg-or-default :onyx.bookkeeper/client-throttle opts)))
  ([zk-addr zk-root-path timeout throttle]
   (let [conf (doto (ClientConfiguration.)
                (.setZkServers zk-addr)
                (.setZkTimeout timeout)
                (.setThrottleValue throttle)
                (.setZkLedgersRootPath zk-root-path))]
     (BookKeeper. conf))))

(def digest-type 
  (BookKeeper$DigestType/MAC))

(defn password [peer-opts]
  (.getBytes ^String (arg-or-default :onyx.bookkeeper/ledger-password peer-opts)))


(defn new-ledger ^LedgerHandle [client peer-opts]
  (let [ensemble-size (arg-or-default :onyx.bookkeeper/ledger-ensemble-size peer-opts)
        quorum-size (arg-or-default :onyx.bookkeeper/ledger-quorum-size peer-opts)]
    (create-ledger client ensemble-size quorum-size digest-type (password peer-opts))))


(def HandleWriteCallback
  (reify AsyncCallback$AddCallback
    (addComplete [this rc lh entry-id callback-fn]
      (callback-fn))))

;; Adapted from Prismatic Plumbing:
;; https://github.com/Prismatic/plumbing/blob/c53ba5d0adf92ec1e25c9ab3b545434f47bc4156/src/plumbing/core.cljx#L346-L361
(defn swap-pair!
  "Like swap! but returns a pair [old-val new-val]"
  ([a f]
     (loop []
       (let [old-val @a
             new-val (f old-val)]
         (if (compare-and-set! a old-val new-val)
           [old-val new-val]
           (recur)))))
  ([a f & args]
     (swap-pair! a #(apply f % args))))

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
        ;; Deref future later so that we can immediately return and continue processing
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
        (close-handle previous-handle)
        (let [compacted {:type :compacted
                         :filter-snapshot @filter-snapshot
                         :extent-state extent-snapshot}
              compacted-ledger (new-ledger client peer-opts)
              compacted-ledger-id (.getId compacted-ledger)]
          (.asyncAddEntry compacted-ledger 
                          ^bytes (nippy/window-log-compress compacted)
                          HandleWriteCallback
                          (fn []
                            (emit-latency-value :window-log-compaction monitoring (- (System/currentTimeMillis) start-time))
                            (>!! outbox-ch
                                 {:fn :compact-bookkeeper-log-ids
                                  :args {:job-id job-id
                                         :task-id task-id
                                         :slot-id slot-id
                                         :peer-id id
                                         :prev-ledger-ids (vec (butlast current-ids))
                                         :new-ledger-ids [compacted-ledger-id]}}))))))))

(defn ch->type [ch batch-ch timeout-ch kill-ch task-kill-ch]
  (cond (= ch timeout-ch)
        :timeout
        (or (= ch kill-ch) (= ch task-kill-ch))
        :shutdown
        :else
        :read))

(defn read-batch [peer-opts batch-ch kill-ch task-kill-ch]
  (let [batch-size (arg-or-default :onyx.bookkeeper/write-batch-size peer-opts)
        timeout-ms (arg-or-default :onyx.bookkeeper/write-batch-timeout peer-opts)
        timeout-ch (timeout timeout-ms)]
    (loop [entries [] ack-fns [] i 0]
      (if (< i batch-size)
        (let [[v ch] (alts!! [batch-ch timeout-ch kill-ch task-kill-ch])
              [entry ack-fn] v]
          (if entry 
            (recur (conj entries entry) 
                   (conj ack-fns ack-fn) 
                   (inc i))
            [(ch->type ch batch-ch timeout-ch kill-ch task-kill-ch) 
             entries
             ack-fns]))
        [:read entries ack-fns]))))

(defn process-batches [{:keys [ledger-handle next-ledger-handle batch-ch] :as log} 
                       {:keys [onyx.core/kill-ch onyx.core/task-kill-ch onyx.core/peer-opts] :as event}]
  (thread 
    (loop [[result batch ack-fns] (read-batch peer-opts batch-ch kill-ch task-kill-ch)]
      ;; Prescence of next-ledger-handle indicates that we should transition to next handle
      ;; and start log compaction at a safe point, i.e. before we write the next entry
      (when @next-ledger-handle
        (compaction-transition log event))
      (when-not (empty? batch) 
        (.asyncAddEntry ^LedgerHandle @ledger-handle 
                        ^bytes (nippy/window-log-compress batch)
                        HandleWriteCallback
                        (fn [] (run! (fn [f] (f)) ack-fns))))
      (if-not (= :shutdown result)
        (recur (read-batch batch-ch kill-ch task-kill-ch))))
    (info "BookKeeper: shutting down batch processing")))

(defmethod state-extensions/initialize-log :bookkeeper [log-type {:keys [onyx.core/replica onyx.core/peer-opts
                                                                         onyx.core/job-id onyx.core/task-id
                                                                         onyx.core/kill-ch onyx.core/task-kill-ch
                                                                         onyx.core/outbox-ch] :as event}] 
  (let [bk-client (bookkeeper peer-opts)
        ledger-handle (new-ledger bk-client peer-opts)
        slot-id (peer-slot-id event)
        new-ledger-id (.getId ledger-handle)
        batch-ch (chan 10000) ;; don't use constant 
        next-ledger-handle nil] 
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
    (info "Ledger id" new-ledger-id "published")
    (doto (->BookKeeperLog bk-client (atom ledger-handle) (atom next-ledger-handle) batch-ch)
      (process-batches event)))) 

(defn playback-windows-extents [event state entry windows]
  (let [grouped-task? (operation/grouped-task? event)
        id->apply-state-update (into {}
                                     (map (juxt :window/id :aggregate/apply-state-update)
                                          windows))]
    (reduce (fn [state' [window-entries {:keys [window/id] :as window}]]
              (reduce (fn [state'' [extent entry grp-key]]
                        (update-in state'' 
                                   [:state id extent]
                                   (fn [ext-state] 
                                     (let [state-value (-> (if grouped-task? (get ext-state grp-key) ext-state)
                                                           (agg/default-state-value window))
                                           apply-fn (id->apply-state-update id)
                                           _ (assert apply-fn (str "Apply fn does not exist for window-id " id))
                                           new-state-value (apply-fn state-value entry)] 
                                       (if grouped-task?
                                         (assoc ext-state grp-key new-state-value)
                                         new-state-value)))))
                      state'
                      window-entries))
            state
            (map list (rest entry) windows))))

(defn compacted-reset? [entry]
  (and (map? entry)
       (= (:type entry) :compacted)))

(defn unpack-compacted [state {:keys [filter-snapshot extent-state]} event]
  (-> state
      (assoc :state extent-state)
      (update :filter state-extensions/restore-filter event filter-snapshot)))

(defn playback-entry [state entry event windows]
  (let [unique-id (first entry)
        _ (trace "Playing back entries for segment with id:" unique-id)
        new-state (playback-windows-extents event state entry windows)]
    (if unique-id
      (update new-state :filter state-extensions/apply-filter-id event unique-id)
      new-state)))

(defn playback-batch-entry [state batch event windows]
  (reduce (fn [state entry]
            (playback-entry state entry event windows))
            state
            batch)) 

(defn playback-ledgers [bk-client peer-opts state ledger-ids {:keys [onyx.core/windows] :as event}]
  (let [pwd (password peer-opts)]
    (reduce (fn [st ledger-id]
              (let [lh (open-ledger bk-client ledger-id digest-type pwd)]
                (try
                  (let [last-confirmed (.getLastAddConfirmed lh)]
                    (info "Opened ledger:" ledger-id "last confirmed:" last-confirmed)
                    (if-not (neg? last-confirmed)
                      (let [entries (.readEntries lh 0 last-confirmed)]
                        (if (.hasMoreElements entries)
                          (loop [st-loop st element ^LedgerEntry (.nextElement entries)]
                            (let [entry-val (nippy/window-log-decompress ^bytes (.getEntry element))
                                  st-loop' (if (compacted-reset? entry-val)
                                             (unpack-compacted st-loop entry-val event)
                                             (playback-batch-entry st-loop entry-val event windows))] 
                              (if (.hasMoreElements entries)
                                (recur st-loop' (.nextElement entries))
                                st-loop')))
                          st))  
                      st))
                  (finally
                    (close-handle lh)))))
            state
            ledger-ids)))

(defmethod state-extensions/playback-log-entries onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [client] :as log} 
   {:keys [onyx.core/monitoring onyx.core/replica 
           onyx.core/peer-opts onyx.core/job-id onyx.core/task-id] :as event} 
   state]
  (emit-latency :window-log-playback 
                monitoring
                (fn [] 
                  (let [slot-id (peer-slot-id event)
                        ;; Don't play back the final ledger id because we just created it
                        prev-ledger-ids (butlast (get-in @replica [:state-logs job-id task-id slot-id]))]
                    (info "Playing back ledgers for" job-id task-id slot-id "ledger-ids" prev-ledger-ids)
                    (playback-ledgers client peer-opts state prev-ledger-ids event)))))

(defmethod state-extensions/compact-log onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [client ledger-handle next-ledger-handle]} 
   {:keys [onyx.core/replica onyx.core/peer-opts
           onyx.core/job-id onyx.core/task-id
           onyx.core/kill-ch onyx.core/task-kill-ch
           onyx.core/outbox-ch] :as event} 
   _] 
  (future
    (let [new-ledger-handle (new-ledger client peer-opts)
          slot-id (peer-slot-id event)
          new-ledger-id (.getId new-ledger-handle)] 
      (>!! outbox-ch
           {:fn :assign-bookkeeper-log-id
            :args {:job-id job-id
                   :task-id task-id
                   :slot-id slot-id
                   :ledger-id new-ledger-id}})
      (while (and (first (alts!! [kill-ch task-kill-ch] :default true))
                  (not= new-ledger-id
                        (last (get-in @replica [:state-logs job-id task-id slot-id]))))
        (info "Transitional GC ledger id has not been published yet. Backing off.")
        (Thread/sleep (arg-or-default :onyx.bookkeeper/ledger-id-written-back-off peer-opts)))
      (reset! next-ledger-handle new-ledger-handle))))

(defmethod state-extensions/close-log onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [client ledger-handle next-ledger-handle]} event] 
  (close-handle @ledger-handle)
  (when @next-ledger-handle
    (close-handle @next-ledger-handle))
  (.close ^BookKeeper client))

(defmethod state-extensions/store-log-entry onyx.state.log.bookkeeper.BookKeeperLog
  [{:keys [ledger-handle next-ledger-handle batch-ch] :as log} event ack-fn entry]
  (>!! batch-ch (list entry ack-fn)))
