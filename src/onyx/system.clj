(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.log.zookeeper :refer [zookeeper]]))

(def development-components [:logging-config :log :queue])

(def peer-components [:logging-config :log :queue :virtual-peer])

(defn rethrow-component [f]
  (try
    (f)
    (catch Exception e
      (.printStackTrace e)
      (throw (.getCause e)))))

(defrecord OnyxDevelopmentEnv []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this development-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this development-components))))

(defrecord OnyxPeer []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this peer-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this peer-components))))

(defn onyx-development-env
  [onyx-id config]
  (map->OnyxDevelopmentEnv
   {:logging-config (logging-config/logging-configuration onyx-id (:logging config))
    :log (component/using (zookeeper onyx-id (:zookeeper config)) [:logging-config])
    :queue (component/using (hornetq onyx-id (:hornetq config)) [:log])}))

(defn onyx-peer
  [onyx-id config opts]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration onyx-id (:logging config))
    :log (component/using (zookeeper onyx-id (:zookeeper config)) [:logging-config])
    :queue (component/using (hornetq onyx-id (:hornetq config)) [:log])
    :virtual-peer (component/using (virtual-peer opts) [:log])}))

