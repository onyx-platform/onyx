(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.sync.zookeeper :refer [zookeeper]]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.logging-configuration :as logging-config]))

(def coordinator-components [:logging-config :sync :queue :coordinator])

(def peer-components [:logging-config :sync :queue :peer])

(defrecord OnyxCoordinator [logging-config sync queue]
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
  [{:keys [log-file log-config hornetq-cluster-name hornetq-group-address
           hornetq-group-port zk-addr onyx-id revoke-delay]}]
  (map->OnyxCoordinator
   {:logging-config (logging-config/logging-configuration log-file log-config)
    :sync (component/using (zookeeper zk-addr onyx-id) [:logging-config])
    :queue (component/using (hornetq hornetq-cluster-name hornetq-group-address hornetq-group-port) [:sync])
    :coordinator (component/using (coordinator revoke-delay) [:sync :queue])}))

(defn onyx-peer
  [{:keys [log-file log-config hornetq-cluster-name hornetq-group-address
           hornetq-group-port zk-addr onyx-id fn-params]}]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration log-file log-config)
    :sync (component/using (zookeeper zk-addr onyx-id) [:logging-config])
    :queue (component/using (hornetq hornetq-cluster-name hornetq-group-address hornetq-group-port) [:sync])
    :peer (component/using (virtual-peer fn-params) [:sync :queue])}))

