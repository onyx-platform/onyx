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
  [{:keys [datomic-uri hornetq-addr zk-addr onyx-id revoke-delay]}]
  (map->OnyxCoordinator
   {:log (datomic datomic-uri (log-schema))
    :sync (zookeeper zk-addr onyx-id)
    :queue (hornetq hornetq-addr)
    :coordinator (component/using (coordinator revoke-delay)
                                  [:log :sync :queue])}))

(defn onyx-peer
  [{:keys [hornetq-addr zk-addr onyx-id]}]
  (map->OnyxPeer
   {:sync (zookeeper zk-addr onyx-id)
    :queue (hornetq hornetq-addr)
    :peer (component/using (virtual-peer)
                           [:sync :queue])}))

