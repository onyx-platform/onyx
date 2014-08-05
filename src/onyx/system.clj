(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.coordinator.election :refer [election]]
            [onyx.coordinator.distributed :refer [coordinator-server]]
            [onyx.sync.zookeeper :refer [zookeeper]]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.logging-configuration :as logging-config]))

(def coordinator-components
  [:logging-config :sync :queue :coordinator])

(def ha-coordinator-components
  [:logging-config :sync :queue :election :coordinator :server])

(def connection-components
  [:logging-config :sync])

(def peer-components
  [:logging-config :sync :queue :peer])

(defn rethrow-component [f]
  (try
    (f)
    (catch Exception e
      (.printStackTrace e)
      (throw (.getCause e)))))

(defrecord OnyxCoordinator []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this coordinator-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this coordinator-components))))

(defrecord OnyxHACoordinator []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this ha-coordinator-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this ha-coordinator-components))))

(defrecord OnyxCoordinatorConnection []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this connection-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this connection-components))))

(defrecord OnyxPeer []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this peer-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this peer-components))))

(defn onyx-coordinator
  [opts]
  (map->OnyxCoordinator
   {:logging-config (logging-config/logging-configuration opts)
    :queue (component/using (hornetq opts) [:logging-config])
    :sync (component/using (zookeeper opts) [:queue :logging-config])
    :coordinator (component/using (coordinator opts) [:sync :queue])}))

(defn onyx-ha-coordinator
  [opts]
  (map->OnyxHACoordinator
   {:logging-config (logging-config/logging-configuration opts)
    :queue (component/using (hornetq opts) [:logging-config])
    :sync (component/using (zookeeper opts) [:queue :logging-config])
    :election (component/using (election opts) [:sync :queue])
    :coordinator (component/using (coordinator opts) [:sync :queue :election])
    :server (component/using (coordinator-server opts) [:coordinator])}))

(defn onyx-coordinator-connection
  [opts]
  (map->OnyxCoordinatorConnection
   {:logging-config (logging-config/logging-configuration opts)
    :sync (component/using (zookeeper opts) [:logging-config])}))

(defn onyx-peer
  [opts]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration opts)
    :queue (component/using (hornetq opts) [:logging-config])
    :sync (component/using (zookeeper opts) [:queue :logging-config])
    :peer (component/using (virtual-peer opts) [:sync :queue])}))

