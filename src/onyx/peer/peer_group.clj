(ns onyx.peer.peer-group
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
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

(defn send-to-outbox! [outbox-ch reactions]
  (doseq [reaction reactions]
    (>!! outbox-ch reaction)))

(defn transition-peers [log entry old-replica new-replica diff peer-config vpeers]
  (reduce
   (fn [result [id vps]]
     (if-let [peer-state (:state (:virtual-peer vps))]
       (let [{:keys [peer-replica-view]} peer-state
             _ (reset! (:replica peer-state) new-replica)
             rs (extensions/reactions entry old-replica new-replica diff peer-state)
             annotated-rs (mapv #(annotate-reaction entry id %) rs)
             ;; This is going to crash when log is not readable.
             ;; Possibly we should just trap it and signal peer-group restart. This will not be necessary after ABS is here
             new-peer-view (extensions/peer-replica-view log entry old-replica new-replica 
                                                         @peer-replica-view diff peer-state peer-config)
             new-state (extensions/fire-side-effects! entry old-replica new-replica diff peer-state)]
         (reset! peer-replica-view new-peer-view)
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
    (info "DISPATCH: " type arg)
    type))

(defmethod action :start-peer-group [{:keys [peer-config command-ch monitoring] :as state} [type arg]]
  (assert (not (:comm state)))
  (let [comm (component/start (comm/onyx-comm peer-config command-ch))
        outbox-ch (:outbox-ch (:log-writer comm))
        group-state {:id (java.util.UUID/randomUUID)
                     :type :group
                     :opts peer-config
                     :log (:log comm)  
                     :command-ch command-ch
                     :monitoring monitoring}]
    (>!! outbox-ch (create-log-entry :prepare-join-cluster {:joiner (:id group-state)}))
    (-> state
        (assoc :replica (:replica-origin (:replica-subscription comm)))
        (assoc :connected? true)
        (assoc :inbox-ch (:inbox-ch (:replica-subscription comm)))
        (assoc :outbox-ch (:outbox-ch (:log-writer comm)))
        (assoc :comm comm)
        (assoc :group-state group-state))))

(defmethod action :stop-peer-group [{:keys [comm peer-owners] :as state} [type arg]]
  ;; TODO: Should emit a group-leave-cluster log message if :connected?
  (try (component/stop comm)
       (catch Throwable t
         (info t "Attempted to stop OnyxComm component failed.")))
  (-> state
      (action [:stop-all-peers])
      (assoc :comm nil)
      (assoc :connected? false)))

(defmethod action :restart-peer-group [state [type arg]]
  (-> state
      (action [:stop-peer-group])
      (action [:start-peer-group])))

(defn safe-stop-vpeer! [vpeer-component]
  (when vpeer-component
    (try
     (component/stop vpeer-component)
     (catch Throwable t
       (info t "Attempt to stop vpeer failed.")))))

(defmethod action :stop-peer [{:keys [outbox-ch group-state] :as state} [type peer-owner-id]]
  (let [vpeer-id (get-in state [:peer-owners peer-owner-id])
        vpeer-component (get-in state [:vpeers vpeer-id])]
    (safe-stop-vpeer! vpeer-component)
    ;when-not (:no-broadcast? component)
    (>!! outbox-ch
         {:fn :leave-cluster
          :args {:id vpeer-id
                 :group-id (:id group-state)}})
    (-> state
        (update :vpeers dissoc vpeer-id)
        (assoc-in [:peer-owners peer-owner-id] nil))))

(defmethod action :stop-all-peers [{:keys [peer-owners] :as state} [_]]
  (reduce (fn [s peer-owner-id]
            (action s [:stop-peer peer-owner-id])) 
          state
          (keys peer-owners)))

(defn send-add-virtual-peer! [outbox-ch vpeer-id group-id peer-site peer-config]
  (send-to-outbox! outbox-ch 
                   [(create-log-entry :add-virtual-peer
                                      {:id vpeer-id
                                       :group-id group-id 
                                       :peer-site peer-site 
                                       :tags (:onyx.peer/tags peer-config)})]))

(defmethod action :start-peer 
  [{:keys [peer-config vpeer-system-fn group-state monitoring 
           connected? messaging-group comm command-ch outbox-ch] :as state} 
   [type peer-owner-id]]
  (if connected?
    (let [vpeer-id (java.util.UUID/randomUUID)
          group-id (:id group-state)
          log (:log comm) 
          vpeer (component/start (vpeer-system-fn command-ch outbox-ch peer-config 
                                                  messaging-group monitoring log group-id vpeer-id))
          peer-site (:peer-site (:virtual-peer vpeer))] 
      (send-add-virtual-peer! outbox-ch vpeer-id group-id peer-site peer-config)
      (-> state 
          (assoc-in [:vpeers vpeer-id] vpeer)
          (assoc-in [:peer-owners peer-owner-id] vpeer-id)))
    state))

(defmethod action :start-all-peers [{:keys [peer-owners] :as state} [_]]
  (reduce (fn [s peer-owner-id]
            (action s [:start-peer peer-owner-id])) 
          state
          (keys peer-owners)))

(defmethod action :restart-peer [state [type peer-owner-id]]
  (-> state
      (action [:stop-peer peer-owner-id])
      (action [:start-peer peer-owner-id])))

(defmethod action :soft-restart-vpeer [state [type peer-owner-id]]
  ;; Just component stop and start here
  )

(defmethod action :add-peer [state [type peer-owner-id]]
  (-> state
      (update :peer-count inc)
      (assoc [:peer-owners peer-owner-id] nil)
      (action [:start-peer peer-owner-id])))

(defmethod action :remove-peer [state [type peer-owner-id]]
  (-> state
      (action [:stop-peer peer-owner-id])
      (update :peer-count dec)
      (update :peer-owners dissoc peer-owner-id)))

(defmethod action :apply-log-entry [{:keys [replica group-state outbox-ch comm peer-config vpeers] :as state} [type entry]]
  (let [new-replica (extensions/apply-log-entry entry replica)
        diff (extensions/replica-diff entry replica new-replica)
        tgroup (transition-group entry replica new-replica diff group-state)
        tpeers (transition-peers (:log comm) entry replica new-replica diff peer-config vpeers)]
    (send-to-outbox! outbox-ch (into (:reactions tgroup) (:reactions tpeers)))
    (-> state
        (assoc :group-state (:group-state tgroup))
        (assoc :vpeers (:vpeers tpeers))
        (assoc :replica new-replica))))

(defn peer-group-manager-loop [state]
  (try 
   (loop [state state]
     (let [{:keys [inbox-ch command-ch shutdown-ch]} state
           chs (if inbox-ch
                 [shutdown-ch command-ch inbox-ch]
                 [shutdown-ch command-ch])
           [entry ch] (alts!! chs :priority true)
           new-state (cond (= ch shutdown-ch)
                           (action state [:stop-peer-group])
                           (= ch command-ch)
                           (action state entry)
                           ;; log reader threw an exception
                           (and (= ch inbox-ch) (instance? java.lang.Throwable entry))
                           (action state [:restart-peer-group])
                           (= ch inbox-ch)
                           (action state [:apply-log-entry entry]))] 
       (when (and new-state (not= ch shutdown-ch))
         (recur new-state))))
   (catch Throwable t
     (error t "Error caught in PeerGroupManager loop."))))

(defrecord PeerGroupManager [peer-config onyx-vpeer-system-fn]
  component/Lifecycle
  (start [{:keys [monitoring messaging-group] :as component}]
    (let [command-ch (chan 1000)
          shutdown-ch (chan 1)
          thread-ch (thread 
                     (>!! command-ch [:start-peer-group])
                     (peer-group-manager-loop {:peer-config peer-config
                                               :vpeer-system-fn onyx-vpeer-system-fn
                                               :connected? false
                                               :group-state nil 
                                               :peer-count 0
                                               :replica nil
                                               :comm nil
                                               :inbox-ch nil
                                               :outbox-ch nil
                                               :shutdown-ch shutdown-ch
                                               :command-ch command-ch
                                               :messaging-group messaging-group
                                               :monitoring monitoring 
                                               :peer-owners {}
                                               :vpeers {}})
                     (info "Dropping out of Peer Group Manager loop"))]
      (assoc component :thread-ch thread-ch :command-ch command-ch :shutdown-ch shutdown-ch)))
  (stop [component]
    (close! (:shutdown-ch component))
    (<!! (:thread-ch component))
    (assoc component :thread-ch nil :command-ch nil :shutdown-ch nil)))

(defn peer-group-manager [peer-config onyx-vpeer-system-fn]
  (->PeerGroupManager peer-config onyx-vpeer-system-fn))
