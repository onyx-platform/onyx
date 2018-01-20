(ns onyx.peer.peer-group-manager
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll! timeout]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [debug info error warn fatal]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.messaging.aeron.messaging-group :refer [media-driver-healthy?]]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.log.curator :as curator]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.communicator :as comm]
            [onyx.extensions :as extensions])
  (:import [java.util.concurrent.locks LockSupport]))

(def media-driver-backoff-ms 500)
(def spin-park-ms 10)
(def peer-group-error-backoff-ms 15000)
(def futures-stuck-park-ms 10000)

(defn annotate-reaction [{:keys [message-id]} id entry]
  (let [peer-annotated (assoc entry :peer-parent id)]
    ;; Not all messages are derived from other messages.
    ;; For instance, :prepare-join-cluster is a "root"
    ;; message.
    (if message-id
      (assoc peer-annotated :entry-parent message-id)
      peer-annotated)))

(defn transition-peers [log entry old-replica new-replica diff peer-config vpeers]
  (reduce
   (fn [result [id vps]]
     (if-let [peer-state (:state (:virtual-peer vps))]
       (let [rs (extensions/reactions entry old-replica new-replica diff peer-state)
             annotated-rs (mapv #(annotate-reaction entry id %) rs)
             _ (reset! (:replica peer-state) new-replica)
             new-state (extensions/fire-side-effects! entry old-replica new-replica diff peer-state)]
         (-> result
             (update-in [:reactions] into annotated-rs)
             (assoc-in [:vpeers id] (assoc-in vps [:virtual-peer :state] new-state))))
       result))
   {:reactions []
    :vpeers vpeers}
   vpeers))

(defn transition-group [entry old-replica new-replica diff group-state]
  (let [rs (extensions/reactions entry old-replica new-replica diff group-state)
        annotated-rs (mapv #(annotate-reaction entry (:id group-state) %) rs)
        new-state (extensions/fire-side-effects! entry old-replica new-replica diff group-state)]
    {:reactions annotated-rs
     :group-state new-state}))

(defmulti action 
  (fn [state [type arg]]
    (debug "ACTION:" (str (:id (:group-state state))) type arg)
    type))

;; ONLY FOR USE IN TESTING
(defmethod action :break-conn [{:keys [comm] :as state} [type arg]]
  (curator/close (:conn (:log comm)))
  state)

(defn start-communicator! [{:keys [peer-config monitoring group-ch] :as state}]
  (assert (not (:comm state)))
  (let [comm (component/start (comm/onyx-comm peer-config group-ch monitoring))]
    (-> state
        (assoc :inbox-ch (:inbox-ch (:replica-subscription comm)))
        (assoc :outbox-ch (:outbox-ch (:log-writer comm)))
        (assoc :replica (:replica-origin (:replica-subscription comm)))
        (assoc :connected? true)
        (assoc :comm comm))))

(defmethod action :start-communicator [state [type _]]
  (start-communicator! state))

(defn setup-group-state [{:keys [comm peer-config group-ch monitoring] :as state}]
  (let [group-id (random-uuid)] 
    (extensions/register-pulse (:log comm) group-id)
    (-> state
        (assoc :group-state {:id group-id
                             :type :group
                             :opts peer-config
                             :log (:log comm)  
                             :group-ch group-ch
                             :monitoring monitoring})
        (action [:send-to-outbox {:fn :prepare-join-cluster 
                                  :args {:joiner group-id}}]))))

(defmethod action :start-peer-group [state [type arg]]
  (if (:up? state) 
    state
    (-> state
        (action [:start-communicator])
        (setup-group-state)
        (action [:start-all-peers])
        (assoc :inbox-entries [])
        (assoc :up? true))))

(defmethod action :stop-communicator [{:keys [comm] :as state} [type arg]]
  (try (component/stop comm)
       (catch Throwable t
         (info t "Attempted to stop OnyxComm component failed.")
         (throw t)))
  (-> state
      (assoc :comm nil)
      (assoc :inbox-ch nil)
      (assoc :outbox-ch nil)
      (assoc :connected? false)))

(defn remove-shutdown-futs [fs]
  (into {} (remove (comp realized? :fut val) fs)))

(defn spin-until-tasks-shutdown [state]
  (let [start-time (System/nanoTime)
        stop-timeout-ns (ms->ns (arg-or-default :onyx.peer/stop-task-timeout-ms (:peer-config state)))]
    (loop [next-state (update state :shutting-down-futures remove-shutdown-futs)]
      (if-not (empty? (:shutting-down-futures next-state))
        (if (< (System/nanoTime) (+ start-time stop-timeout-ns))
          (do (LockSupport/parkNanos (ms->ns spin-park-ms))
              (recur (update next-state :shutting-down-futures remove-shutdown-futs)))
          (do (info "WARNING: stopping tasks exceeded :onyx.peer/stop-task-timeout-ms")
              (LockSupport/parkNanos (ms->ns futures-stuck-park-ms))
              (update next-state :shutting-down-futures remove-shutdown-futs)))
        next-state))))

(defn shutting-down-task-metrics [{:keys [shutting-down-futures set-num-peer-shutdowns! set-peer-shutdown-duration-ms!] :as state}]
  (set-peer-shutdown-duration-ms!
   (if (empty? shutting-down-futures)
     0
     (let [t (System/nanoTime)] 
       (apply max
              (mapv (fn [{:keys [time]}]
                      (long (/ (- t time) 1000000))) 
                    (vals shutting-down-futures))))))
  
  (set-num-peer-shutdowns! (count shutting-down-futures))
  state)

(defmethod action :stop-peer-group [state [type arg]]
  (if (:up? state)
    (do
     (-> state
         (action [:stop-all-peers])
         ;; Allow this to be overridden and see if peer is kicked off?
         (action [:send-to-outbox {:fn :group-leave-cluster :args {:id (:id (:group-state state))}
                                   :peer-parent (:id (:group-state state))}])
         (action [:stop-communicator])
         (assoc :inbox-entries [])
         (spin-until-tasks-shutdown)
         (shutting-down-task-metrics)
         (assoc :up? false)))
    state))

(defmethod action :restart-peer-group [state [type group-id]]
  ;; Only restart if group-id is not supplied, or if group-id is supplied
  ;; and we haven't restarted yet
  (if (or (nil? group-id) 
          (= group-id (:id (:group-state state))))
    (-> state
        (action [:stop-peer-group])
        (action [:start-peer-group]))
    state))

(defn safe-stop-vpeer! [vpeer-component]
  (when vpeer-component
    (try
      (component/stop vpeer-component)
      (catch Throwable t
        (info t "Attempt to stop vpeer failed.")))))

(defmethod action :stop-peer [{:keys [group-state] :as state} [type peer-owner-id]]
  (let [vpeer-id (get-in state [:peer-owners peer-owner-id])
        vpeer-component (get-in state [:vpeers vpeer-id])]
    (safe-stop-vpeer! vpeer-component)
    (-> state
        (update-in [:vpeers] dissoc vpeer-id)
        (assoc-in [:peer-owners peer-owner-id] nil))))

(defmethod action :stop-all-peers [{:keys [peer-owners] :as state} [_]]
  (reduce (fn [s peer-owner-id]
            (action s [:stop-peer peer-owner-id])) 
          state
          (keys peer-owners)))

(defmethod action :stop-task-lifecycle
  [state [type [id time-stopped fut]]]
  (update state 
          :shutting-down-futures 
          assoc 
          id 
          {:time time-stopped 
           :id id
           :fut fut}))

(defmethod action :send-to-outbox
  [{:keys [outbox-ch] :as state} [type entry]]
  (>!! outbox-ch entry)
  state)

(defmethod action :start-peer
  [{:keys [peer-config vpeer-system-fn group-state monitoring 
           connected? messenger-group state-store-group comm group-ch outbox-ch] :as state} 
   [type peer-owner-id]]
  (if connected?
    (let [vpeer-id (random-uuid)
          group-id (:id group-state)
          log (:log comm) 
          vpeer (component/start (vpeer-system-fn group-ch outbox-ch peer-config 
                                                  messenger-group state-store-group
                                                  monitoring log group-id vpeer-id))] 
      (-> state 
          (assoc-in [:vpeers vpeer-id] vpeer)
          (assoc-in [:peer-owners peer-owner-id] vpeer-id)))
    state))

(defmethod action :start-all-peers [{:keys [peer-owners] :as state} [_]]
  (reduce (fn [s peer-owner-id]
            (action s [:start-peer peer-owner-id])) 
          state
          (keys peer-owners)))

(defmethod action :restart-peer [{:keys [peer-owners] :as state} [type peer-owner-id]]
  (assert peer-owner-id)
  (if (get peer-owners peer-owner-id) 
    (-> state
        (action [:stop-peer peer-owner-id])
        (action [:start-peer peer-owner-id]))
    state))

(defmethod action :restart-vpeer [{:keys [peer-owners] :as state} [type peer-id]]
  (assert peer-id)
  (if-let [peer-owner (get (clojure.set/map-invert peer-owners) peer-id)]
    (action state [:restart-peer peer-owner])
    state))

(defmethod action :add-peer [state [type peer-owner-id]]
  (if-not (get-in state [:peer-owners peer-owner-id])
    (-> state
        (update :peer-count inc)
        (assoc-in [:peer-owners peer-owner-id] nil)
        (action [:start-peer peer-owner-id]))
    state))

(defmethod action :remove-peer [state [type peer-owner-id]]
  (if (get-in state [:peer-owners peer-owner-id]) 
    (-> state
        (action [:stop-peer peer-owner-id])
        (update :peer-count dec)
        (update :peer-owners dissoc peer-owner-id))
    state))

(defn peers-allocated-proportion [{:keys [group-state replica up?] :as state}]
  (if up?
    (let [allocated-peers (reduce into #{} (mapcat vals (vals (:allocations replica))))
          our-peers (get-in replica [:groups-index (:id group-state)])]
      (if (empty? our-peers)
        0
        (double (/ (count (filter allocated-peers our-peers)) 
                   (count our-peers)))))
    0))


(defmethod action :monitor [{:keys [peer-group-heartbeat!] :as state} _]
  (peer-group-heartbeat!)
  (let [state-shutdown (-> state 
                           (update :shutting-down-futures remove-shutdown-futs)
                           (shutting-down-task-metrics))]
    (cond (and (:up? state) (not (media-driver-healthy?)))
          (do
           (warn "Aeron media driver has not started up, thus stopping all peers until it's up again.")
           (action state-shutdown [:stop-peer-group]))

          (and (not (:up? state)) (media-driver-healthy?))
          (do
           (warn "Aeron media driver is healthy, thus starting all peers.")
           (action state-shutdown [:start-peer-group]))

          :else
          (do
           (when-not (media-driver-healthy?)
             (warn "Aeron media driver has not started up. Waiting for media driver before starting peers, and backing off for 500ms.")
             (LockSupport/parkNanos (ms->ns media-driver-backoff-ms)))
           state-shutdown))))

(defn update-scheduler-lag! [{:keys [set-scheduler-lag! inbox-entries]}]
  (if (> (count inbox-entries) 1) 
    (set-scheduler-lag! (- (:created-at (last inbox-entries)) (:created-at (first inbox-entries))))
    (set-scheduler-lag! 0)))

(defmethod action :apply-log-entry 
  [{:keys [replica group-state comm peer-config state-store-group
           vpeers query-server messenger-group inbox-entries set-peer-group-allocation-proportion!] :as state}
   [type]]
  (let [entry (first inbox-entries)]
    (if (instance? java.lang.Throwable entry)
    (action state [:restart-peer-group])
    (try 
     (let [_ (update-scheduler-lag! state)
           new-replica (extensions/apply-log-entry entry (assoc replica :version (:message-id entry))) 
           diff (extensions/replica-diff entry replica new-replica)
           tgroup (transition-group entry replica new-replica diff group-state)
           tpeers (transition-peers (:log comm) entry replica new-replica diff peer-config vpeers)
           reactions (into (:reactions tgroup) (:reactions tpeers))]
       (when-let [deallocated (first (clojure.data/diff (:allocation-version replica) 
                                                        (:allocation-version new-replica)))] 
         (>!! (:ch state-store-group) [:drop-job-dbs deallocated]))
       (update query-server :replica reset! new-replica)
       (update messenger-group :replica reset! new-replica)
       (let [next-state (as-> state st
                          (update st :inbox-entries (comp vec rest))
                          (reduce (fn [s r] (action s [:send-to-outbox r])) st reactions)
                          (assoc st :group-state (:group-state tgroup))
                          (assoc st :vpeers (:vpeers tpeers))
                          (assoc st :replica new-replica))]
         (set-peer-group-allocation-proportion! (peers-allocated-proportion next-state))
         next-state))
     (catch Exception e
       ;; Stateful things happen in the transitions.
       ;; Need to reboot entire peer group.
       ;; Future work should eliminate uncertainty here e.g. use of log in transition-peers
       (error e (format "Error applying log entry: %s to %s. Rebooting peer-group %s."
                        entry
                        replica 
                        (:id group-state)))
       (action state [:restart-peer-group (:id group-state)]))))))

(def idle-backoff-ms 10)

(defn poll-inbox! [{:keys [inbox-ch] :as state}]
  (if inbox-ch
    (update state 
            :inbox-entries 
            into 
            (loop [entries []]
              (if-let [v (poll! inbox-ch)]
                (recur (conj entries v))
                entries)))
    state))

(defn peer-group-manager-loop [state]
  (try 
   (loop [state state]
     (let [{:keys [group-ch shutdown-ch]} state
           [entry ch] (alts!! [group-ch shutdown-ch] :priority true :default :default)
           new-state (cond (= ch shutdown-ch)
                           (action state [:stop-peer-group])

                           (= ch group-ch)
                           (-> state 
                               (action entry)
                               (action [:monitor]))

                           (= entry :default)
                           (let [next-state (poll-inbox! state)]
                             (if (empty? (:inbox-entries state))
                               (let [next-state* (action next-state [:monitor])]
                                 (LockSupport/parkNanos (ms->ns idle-backoff-ms))
                                 next-state*)
                               (-> next-state 
                                   (action [:apply-log-entry]) 
                                   (action [:monitor])))))] 
       (if (and new-state (not= ch shutdown-ch))
         (recur new-state))))
   (catch Exception t
     (error t (format "Unrecoverable error caught in PeerGroupManager loop. Exiting."))
     (System/exit 1))))

(defrecord PeerGroupManager [peer-config onyx-vpeer-system-fn]
  component/Lifecycle
  (start [{:keys [monitoring query-server messenger-group state-store-group] :as component}]
    (let [group-ch (chan 1000)
          shutdown-ch (chan 1)
          initial-state (merge {:peer-config peer-config
                                :vpeer-system-fn onyx-vpeer-system-fn
                                :up? false
                                :connected? false
                                :group-state nil 
                                :peer-count 0
                                :replica nil
                                :comm nil
                                :shutting-down-futures {}
                                :inbox-entries []
                                :inbox-ch nil
                                :outbox-ch nil
                                :shutdown-ch shutdown-ch
                                :group-ch group-ch
                                :state-store-group state-store-group
                                :messenger-group messenger-group
                                :monitoring monitoring
                                :query-server query-server
                                :peer-owners {}
                                :vpeers {}}
                               (select-keys monitoring
                                            [:set-peer-shutdown-duration-ms!
                                             :set-peer-group-allocation-proportion!
                                             :set-scheduler-lag!
                                             :set-num-peer-shutdowns!
                                             :peer-group-heartbeat!]))
          thread-ch (thread (peer-group-manager-loop initial-state)
                            (info "Dropping out of Peer Group Manager loop"))]
      (assoc component :thread-ch thread-ch :group-ch group-ch 
             :initial-state initial-state :shutdown-ch shutdown-ch)))
  (stop [component]
    (close! (:shutdown-ch component))
    (<!! (:thread-ch component))
    (assoc component :thread-ch nil :group-ch nil :shutdown-ch nil :initial-state nil)))

(defn peer-group-manager [peer-config onyx-vpeer-system-fn]
  (->PeerGroupManager peer-config onyx-vpeer-system-fn))
