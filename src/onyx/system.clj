(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.sync.zookeeper :refer [zookeeper]]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]))

(def coordinator-components [:log :sync :queue :coordinator])

(def peer-components [:sync :queue :peer])

(defrecord OnyxCoordinator [log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this coordinator-components))
  (stop [this]
    (component/stop-system this coordinator-components)))

(defrecord OnyxPeer [sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this peer-components))
  (stop [this]
    (component/stop-system this peer-components)))

(defn onyx-coordinator
  [{:keys [datomic-uri hornetq-host hornetq-port zk-addr onyx-id revoke-delay]}]
  (map->OnyxCoordinator
   {:log (component/using (datomic datomic-uri (log-schema)) [:log])
    :sync (component/using (zookeeper zk-addr onyx-id) [:log])
    :queue (component/using (hornetq hornetq-host hornetq-port) [:log])
    :coordinator (component/using (coordinator revoke-delay)
                                  [:log :sync :queue])}))

(defn onyx-peer
  [{:keys [hornetq-host hornetq-port zk-addr onyx-id]}]
  (map->OnyxPeer
   {:sync (component/using (zookeeper zk-addr onyx-id) [])
    :queue (component/using (hornetq hornetq-host hornetq-port) [:sync])
    :peer (component/using (virtual-peer)
                           [:sync :queue])}))

