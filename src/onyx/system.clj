(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal]]
            [onyx.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.messaging.messaging-buffer :refer [messaging-buffer]]
            [onyx.messaging.http-kit :refer [http-kit]]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.log.commands.prepare-join-cluster]
            [onyx.log.commands.accept-join-cluster]
            [onyx.log.commands.abort-join-cluster]
            [onyx.log.commands.notify-join-cluster]
            [onyx.log.commands.leave-cluster]
            [onyx.log.commands.submit-job]
            [onyx.log.commands.volunteer-for-task]
            [onyx.log.commands.seal-task]
            [onyx.log.commands.complete-task]
            [onyx.log.commands.kill-job]
            [onyx.log.commands.gc]))

(def development-components [:logging-config :log :messaging-buffer :messaging :queue])

(def client-components [:logging-config :log :messaging-buffer :messaging :queue])

(def peer-components [:logging-config :log :messaging-buffer :messaging :queue :virtual-peer])

(def messaging
  {:http-kit http-kit})

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

(defrecord OnyxClient []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this client-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this client-components))))

(defrecord OnyxPeer []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this peer-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this peer-components))))

(defrecord OnyxFakePeer []
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
    :messaging-buffer (component/using (messaging-buffer config) [:log])
    :messaging (component/using (get messaging (:onyx.messaging/impl config)) [:messaging-buffer])
    :queue (component/using (hornetq config) [:log :messaging-buffer])}))

(defn onyx-client
  [config]
  (map->OnyxClient
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :messaging-buffer (component/using (messaging-buffer config) [:log])
    :messaging (component/using (get messaging (:onyx.messaging/impl config)) [:messaging-buffer])
    :queue (component/using (hornetq config) [:log :messaging-buffer])}))

(defn onyx-peer
  [config]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :messaging-buffer (component/using (messaging-buffer config) [:log])
    :messaging (component/using (get messaging (:onyx.messaging/impl config)) [:messaging-buffer])
    :queue (component/using (hornetq config) [:messaging-buffer])
    :virtual-peer (component/using (virtual-peer config) [:log :queue])}))

(defrecord FakeHornetQConnection []
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Fake HornetQ connection")
    component)

  (stop [component]
    (taoensso.timbre/info "Stopping Fake HornetQ connection")
    component))

(defn fake-hornetq [_]
  (map->FakeHornetQConnection {}))

(defn onyx-fake-peer
  [config]
  (map->OnyxFakePeer
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :messaging-buffer (component/using (messaging-buffer config) [:log])
    :messaging (component/using (get messaging (:onyx.messaging/impl config)) [:messaging-buffer])
    :queue (component/using (fake-hornetq config) [:messaging-buffer :log])
    :virtual-peer (component/using (virtual-peer config) [:log :queue])}))

