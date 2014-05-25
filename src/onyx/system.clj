(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.log.datomic :refer [datomic log-schema]]
            [onyx.sync.zookeeper :refer [zookeeper]]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.logging-configuration :as logging-config]))

(def coordinator-components [:logging-config :log :sync :queue :coordinator])

(def peer-components [:logging-config :sync :queue :peer])

(defrecord OnyxCoordinator [logging-config log sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this coordinator-components))
  (stop [this]
    (component/stop-system this coordinator-components)))

(defrecord OnyxPeer [logging-config sync queue]
  component/Lifecycle
  (start [this]
    (component/start-system this peer-components))
  (stop [this]
    (component/stop-system this peer-components)))

(defn onyx-coordinator
  [{:keys [log-file datomic-uri hornetq-host hornetq-port zk-addr onyx-id revoke-delay]}]
  (map->OnyxCoordinator
   {:logging-config (logging-config/logging-configuration log-file)
    :log (component/using (datomic datomic-uri (log-schema)) [:logging-config])
    :sync (component/using (zookeeper zk-addr onyx-id) [:log])
    :queue (component/using (hornetq hornetq-host hornetq-port) [:log])
    :coordinator (component/using (coordinator revoke-delay) [:log :sync :queue])}))

(defn onyx-peer
  [{:keys [log-file hornetq-host hornetq-port zk-addr onyx-id fn-params]}]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration log-file)
    :sync (component/using (zookeeper zk-addr onyx-id) [:logging-config])
    :queue (component/using (hornetq hornetq-host hornetq-port) [:sync])
    :peer (component/using (virtual-peer fn-params) [:sync :queue])}))

