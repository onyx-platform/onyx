(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.messaging.acking-daemon :refer [acking-daemon]]
            [onyx.messaging.messenger-buffer :refer [messenger-buffer]]
            [onyx.messaging.http-kit :refer [http-kit-websockets]]
            [onyx.messaging.aleph :refer [aleph-tcp-sockets]]
            [onyx.messaging.aeron :refer [aeron]]
            [onyx.messaging.netty-tcp :refer [netty-tcp-sockets]]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.log.commands.prepare-join-cluster]
            [onyx.log.commands.accept-join-cluster]
            [onyx.log.commands.abort-join-cluster]
            [onyx.log.commands.notify-join-cluster]
            [onyx.log.commands.exhaust-input]
            [onyx.log.commands.seal-output]
            [onyx.log.commands.signal-ready]
            [onyx.log.commands.leave-cluster]
            [onyx.log.commands.submit-job]
            [onyx.log.commands.volunteer-for-task]
            [onyx.log.commands.kill-job]
            [onyx.log.commands.gc]))

(def development-components [:logging-config :log])

(def client-components [:logging-config :log])

(def peer-components [:logging-config :log :messenger-buffer :messenger :acking-daemon :virtual-peer])

(def messenger
  {:http-kit-websockets http-kit-websockets
   :netty-tcp netty-tcp-sockets
   :aleph-tcp aleph-tcp-sockets
   :aeron aeron})

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

(defn messenger-ctor [config]
  (let [rets ((get messenger (:onyx.messaging/impl config)) config)]
    (when-not rets
      (throw (ex-info "Could not find Messaging implementation" {:impl (:onyx.messaging/impl config)})))
    rets))

(defn onyx-development-env
  [config]
  (map->OnyxDevelopmentEnv
   {:logging-config (logging-config/logging-configuration config)
    :log (component/using (zookeeper config) [:logging-config])}))

(defn onyx-client
  [config]
  (map->OnyxClient
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])}))

(defn onyx-peer
  [config]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :acking-daemon (component/using (acking-daemon config) [:log])
    :messenger-buffer (component/using (messenger-buffer config) [:log :acking-daemon])
    :messenger (component/using (messenger-ctor config) [:acking-daemon :messenger-buffer])
    :virtual-peer (component/using (virtual-peer config) [:log :acking-daemon :messenger-buffer :messenger])}))

