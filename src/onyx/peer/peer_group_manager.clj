(ns onyx.peer.peer-group-manager
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [debug info error warn fatal]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.log.curator :as curator]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.communicator :as comm]
            [onyx.extensions :as extensions]))

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
             new-state (extensions/fire-side-effects! entry old-replica new-replica diff peer-state)]
         (reset! (:replica peer-state) new-replica)
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
    (info "ACTION:" (:id (:group-state state)) type arg)
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
  (if (:stopped? state) 
    (-> state
        (action [:start-communicator])
        (setup-group-state)
        (action [:start-all-peers])
        (assoc :stopped? false))
    state))

(defmethod action :stop-communicator [{:keys [comm] :as state} [type arg]]
  (try (component/stop comm)
       (catch Throwable t
         (info t "Attempted to stop OnyxComm component failed.")))
  (-> state
      (assoc :comm nil)
      (assoc :connected? false)))

(defmethod action :stop-peer-group [state [type arg]]
  (if (:stopped? state) 
    state
    (-> state
        (action [:stop-all-peers])
        ;; Allow this to be overridden and see if peer is kicked off?
        (action [:send-to-outbox {:fn :group-leave-cluster :args {:id (:id (:group-state state))}
                                  :peer-parent (:id (:group-state state))}])
        (action [:stop-communicator])
        (assoc :stopped? true))))

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

(defmethod action :send-to-outbox
  [{:keys [outbox-ch] :as state} [type entry]]
  (>!! outbox-ch entry)
  state)

(defmethod action :start-peer
  [{:keys [peer-config vpeer-system-fn group-state monitoring 
           connected? messenger-group comm group-ch outbox-ch] :as state} 
   [type peer-owner-id]]
  (if connected?
    (let [vpeer-id (random-uuid)
          group-id (:id group-state)
          log (:log comm) 
          vpeer (component/start (vpeer-system-fn group-ch outbox-ch peer-config 
                                                  messenger-group monitoring log group-id vpeer-id))] 
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

(defmethod action :apply-log-entry [{:keys [replica group-state comm peer-config vpeers] :as state} [type entry]]
  (try 
   (let [new-replica (extensions/apply-log-entry entry (assoc replica :version (:message-id entry))) 
         diff (extensions/replica-diff entry replica new-replica)
         tgroup (transition-group entry replica new-replica diff group-state)
         tpeers (transition-peers (:log comm) entry replica new-replica diff peer-config vpeers)
         reactions (into (:reactions tgroup) (:reactions tpeers))]
     (-> (reduce (fn [s r] (action s [:send-to-outbox r])) state reactions)
         (assoc :group-state (:group-state tgroup))
         (assoc :vpeers (:vpeers tpeers))
         (assoc :replica new-replica)))
   (catch Exception e
     ;; Stateful things happen in the transitions.
     ;; Need to reboot entire peer group.
     ;; Future work should eliminate uncertainty here e.g. use of log in transition-peers
     (error e (format "Error applying log entry: %s to %s. Rebooting peer-group %s." entry replica (:id group-state)) e)
     (action state [:restart-peer-group (:id group-state)]))))

(defn peer-group-manager-loop [state]
  (try 
   (loop [state (action state [:start-peer-group])]
     (let [{:keys [inbox-ch group-ch shutdown-ch]} state
           chs (if inbox-ch
                 [shutdown-ch group-ch inbox-ch]
                 [shutdown-ch group-ch])
           [entry ch] (alts!! chs :priority true)
           new-state (cond (= ch shutdown-ch)
                           (action state [:stop-peer-group])
                           (= ch group-ch)
                           (action state entry)
                           ;; log reader threw an exception
                           (and (= ch inbox-ch) (instance? java.lang.Throwable entry))
                           (action state [:restart-peer-group])
                           (= ch inbox-ch)
                           (action state [:apply-log-entry entry]))] 
       (when (and new-state (not= ch shutdown-ch))
         (recur new-state))))
   (catch Throwable t
     (error "Error caught in PeerGroupManager loop." t))))

(defn initial-state 
  [peer-config onyx-vpeer-system-fn shutdown-ch group-ch messenger-group monitoring]
  {:peer-config peer-config
   :vpeer-system-fn onyx-vpeer-system-fn
   :stopped? true
   :connected? false
   :group-state nil 
   :peer-count 0
   :replica nil
   :comm nil
   :inbox-ch nil
   :outbox-ch nil
   :shutdown-ch shutdown-ch
   :group-ch group-ch
   :messenger-group messenger-group
   :monitoring monitoring 
   :peer-owners {}
   :vpeers {}})

(defrecord PeerGroupManager [peer-config onyx-vpeer-system-fn]
  component/Lifecycle
  (start [{:keys [monitoring messenger-group] :as component}]
    (let [;; FIXME, move into information model
          group-ch (chan 100000)
          shutdown-ch (chan 1)
          state (initial-state peer-config
                               onyx-vpeer-system-fn
                               shutdown-ch
                               group-ch
                               messenger-group
                               monitoring)
          thread-ch (thread (peer-group-manager-loop state)
                            (info "Dropping out of Peer Group Manager loop"))]
      (assoc component 
             :initial-state state :thread-ch thread-ch 
             :group-ch group-ch :shutdown-ch shutdown-ch)))
  (stop [component]
    (close! (:shutdown-ch component))
    (<!! (:thread-ch component))
    (assoc component :thread-ch nil :group-ch nil :shutdown-ch nil :initial-state nil)))

(defn peer-group-manager [peer-config onyx-vpeer-system-fn]
  (->PeerGroupManager peer-config onyx-vpeer-system-fn))
