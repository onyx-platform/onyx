(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.logging-configuration :as logging-config]))

(def development-components [:logging-config :log])

(def peer-components [:logging-config :log])

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
  [id config]
  (map->OnyxDevelopmentEnv
   {:logging-config (logging-config/logging-configuration id (:logging config))
    :log (component/using (zookeeper id (:zookeeper config)) [:logging-config])}))

(defn onyx-peer
  [id config]
  (map->OnyxDevelopmentEnv
   {:logging-config (logging-config/logging-configuration id (:logging config))
    :log (component/using (zookeeper id (:zookeeper config)) [:logging-config])}))

