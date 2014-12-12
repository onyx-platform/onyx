(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal]]
            [onyx.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.log.commands.prepare-join-cluster]
            [onyx.log.commands.accept-join-cluster]
            [onyx.log.commands.abort-join-cluster]
            [onyx.log.commands.notify-join-cluster]
            [onyx.log.commands.leave-cluster]
            [onyx.log.commands.submit-job]
            [onyx.log.commands.volunteer-for-task]
            [onyx.log.commands.update-peer-state]
            [onyx.log.commands.seal-task]
            [onyx.log.commands.complete-task]))

(def development-components [:logging-config :log :queue])

(def peer-components [:logging-config :log :queue :virtual-peer])

(defn rethrow-component [f]
  (try
    (f)
    (catch Exception e
      (fatal e)
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
  [config]
  (map->OnyxDevelopmentEnv
   {:logging-config (logging-config/logging-configuration config)
    :log (component/using (zookeeper config) [:logging-config])
    :queue (component/using (hornetq config) [:log])}))

(defn onyx-peer
  [config]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :queue (component/using (hornetq config) [:log])
    :virtual-peer (component/using (virtual-peer config) [:log :queue])}))

