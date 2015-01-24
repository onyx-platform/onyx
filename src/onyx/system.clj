(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal]]
            [onyx.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.messaging.messenger-buffer :refer [messenger-buffer]]
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

(def development-components [:logging-config :log :messenger-buffer :messenger])

(def client-components [:logging-config :log :messenger-buffer :messenger])

(def peer-components [:logging-config :log :messenger-buffer :messenger :virtual-peer])

(def messenger
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

(defn messenger-ctor [config]
  (let [rets ((get messenger (:onyx.messaging/impl config)) config)]
    (when-not rets
      (throw (ex-info "Could not find Messaging implementation" {:impl (:onyx.messaging/impl config)})))
    rets))

(defn onyx-development-env
  [config]
  (map->OnyxDevelopmentEnv
   {:logging-config (logging-config/logging-configuration config)
    :log (component/using (zookeeper config) [:logging-config])
    :messenger-buffer (component/using (messenger-buffer config) [:log])
    :messenger (component/using (messenger-ctor config) [:messenger-buffer])}))

(defn onyx-client
  [config]
  (map->OnyxClient
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :messenger-buffer (component/using (messenger-buffer config) [:log])
    :messenger (component/using (messenger-ctor config) [:messenger-buffer])}))

(defn onyx-peer
  [config]
  (map->OnyxPeer
   {:logging-config (logging-config/logging-configuration (:logging config))
    :log (component/using (zookeeper config) [:logging-config])
    :messenger-buffer (component/using (messenger-buffer config) [:log])
    :messenger (component/using (messenger-ctor config) [:messenger-buffer])
    :virtual-peer (component/using (virtual-peer config) [:log :messenger :messenger-buffer])}))

