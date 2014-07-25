(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.async :refer [coordinator]]
            [onyx.sync.zookeeper :refer [zookeeper]]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.logging-configuration :as logging-config]))

(def coordinator-components [:logging-config :sync :queue :coordinator])

(def peer-components [:logging-config :sync :queue :peer])

(defn rethrow-component [f]
  (try
    (f)
    (catch Exception e
      (throw (.getCause e)))))

(defrecord OnyxCoordinator [logging-config sync queue]
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this coordinator-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this coordinator-components))))

(defrecord OnyxCoordinatorConnection [logging-config sync]
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this connection-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this connection-components))))

(defrecord OnyxPeer [logging-config sync queue]
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
    :sync (component/using (zookeeper opts) [:logging-config])
    :queue (component/using (hornetq opts) [:sync])
    :coordinator (component/using (coordinator opts) [:sync :queue])}))

(defn onyx-peer
  [opts]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration opts)
    :sync (component/using (zookeeper opts) [:logging-config])
    :queue (component/using (hornetq opts) [:sync])
    :peer (component/using (virtual-peer opts) [:sync :queue])}))

