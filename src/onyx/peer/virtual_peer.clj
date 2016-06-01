(ns ^:no-doc onyx.peer.virtual-peer
  (:require [clojure.core.async :refer [chan >!! <!! thread alts!! close! dropping-buffer]]
            [com.stuartsierra.component :as component]
            [onyx.extensions :as extensions]
            [taoensso.timbre :as timbre]
            [onyx.peer.operation :as operation]
            [onyx.messaging.aeron :as am]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.static.default-vals :refer [defaults arg-or-default]]))

(defn join-entry [state]
  {:fn :add-virtual-peer 
   :args {:id (:id state)
          :group-id (:group-id state)
          :peer-site (:peer-site state)
          :tags (:onyx.peer/tags (:peer-config state))}})

(defrecord VirtualPeer [peer-config task-component-fn id]
  component/Lifecycle

  (start [{:keys [group-id logging-config monitoring log acking-daemon
                  messenger virtual-peers replica-subscription replica-chamber]
           :as component}]
    (taoensso.timbre/info (format "Starting Virtual Peer %s" id))
    (let [peer-site (extensions/peer-site messenger)
          state
          (atom (merge {:id id
                        :type :peer
                        :group-id group-id
                        :task-component-fn task-component-fn
                        :replica (:replica replica-subscription)
                        :peer-replica-view (atom {})
                        :log log
                        :messenger messenger
                        :monitoring monitoring
                        :opts peer-config
                        :outbox-ch (:outbox-ch replica-chamber)
                        :completion-ch (:completion-ch acking-daemon)
                        :logging-config logging-config
                        :peer-site peer-site
                        :vpeers (:vpeer-systems virtual-peers)}
                       (:onyx.peer/state peer-config)))]
      (>!! (:outbox-ch replica-chamber)
           (create-log-entry
            :add-virtual-peer
            {:id id 
             :group-id group-id 
             :peer-site peer-site 
             :tags (:onyx.peer/tags peer-config)}))
      (assoc component
             :id id
             :group-id group-id
             :peer-config peer-config
             :peer-site peer-site
             :outbox-ch (:outbox-ch replica-chamber)
             :state state)))

  (stop [component]
    (taoensso.timbre/info (format "Stopping Virtual Peer %s" (:id component)))
    (let [vps (:vpeer-systems (:virtual-peers component))]
      (swap! vps dissoc (:id component))
      (swap! (:state component)
             (fn [state-snapshot]
               (when-let [f (:lifecycle-stop-fn state-snapshot)]
                 (f :peer-left))
               nil))

      (when-not (:no-broadcast? component)
        (>!! (:outbox-ch component)
             {:fn :leave-cluster
              :args {:id (:id component)
                     :group-id (:group-id component)}}))
      (assoc component :state nil))))

(defmethod clojure.core/print-method VirtualPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Virtual Peer>"))

(defn virtual-peer
  [peer-config task-component-fn id]
  (map->VirtualPeer {:id id
                     :peer-config peer-config
                     :task-component-fn task-component-fn}))

(defrecord VirtualPeers [peer-config]
  component/Lifecycle

  (start [{:keys [peer-config monitoring messaging-group] :as component}]
    (let [group-id (java.util.UUID/randomUUID)]
      (assoc component :group-id group-id :vpeer-systems (atom {}))))

  (stop [component]
    component))

(defn virtual-peers [peer-config]
  (->VirtualPeers peer-config))
