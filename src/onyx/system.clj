(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.sync.zookeeper :refer [zookeeper]]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.logger :refer [logger]]))

(def coordinator-components [:logger :log :sync :queue :coordinator])

(def peer-components [:logger :sync :queue :peer])

(defrecord OnyxCoordinator [logger log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this coordinator-components))
  (stop [this]
    (component/stop-system this coordinator-components)))

(defrecord OnyxPeer [logger sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this peer-components))
  (stop [this]
    (component/stop-system this peer-components)))

(defn onyx-coordinator
  [{:keys [datomic-uri hornetq-addr zk-addr onyx-id revoke-delay]}]
  (map->OnyxCoordinator
   {:logger (logger)
    :log (component/using (datomic datomic-uri (log-schema)) [:logger])
    :sync (component/using (zookeeper zk-addr onyx-id) [:logger])
    :queue (component/using (hornetq hornetq-addr) [:logger])
    :coordinator (component/using (coordinator revoke-delay)
                                  [:logger :log :sync :queue])}))

(defn onyx-peer
  [{:keys [hornetq-addr zk-addr onyx-id]}]
  (map->OnyxPeer
   {:logger (logger)
    :sync (component/using (zookeeper zk-addr onyx-id) [:logger])
    :queue (component/using (hornetq hornetq-addr) [:logger])
    :peer (component/using (virtual-peer)
                           [:logger :sync :queue])}))

