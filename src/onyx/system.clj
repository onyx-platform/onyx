(ns onyx.system
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.peer.task-lifecycle :refer [task-lifecycle]]
            [onyx.peer.backpressure-poll :refer [backpressure-poll]]
            [onyx.messaging.acking-daemon :refer [acking-daemon]]
            [onyx.messaging.common :refer [messenger messaging-require messaging-peer-group]]
            [onyx.messaging.messenger-buffer :as buffer]
            [onyx.monitoring.no-op-monitoring]
            [onyx.monitoring.custom-monitoring]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.state.bookkeeper :refer [new-bookie]]
            [onyx.log.commands.prepare-join-cluster]
            [onyx.log.commands.accept-join-cluster]
            [onyx.log.commands.abort-join-cluster]
            [onyx.log.commands.notify-join-cluster]
            [onyx.log.commands.exhaust-input]
            [onyx.log.commands.seal-output]
            [onyx.log.commands.signal-ready]
            [onyx.log.commands.set-replica]
            [onyx.log.commands.leave-cluster]
            [onyx.log.commands.submit-job]
            [onyx.log.commands.kill-job]
            [onyx.log.commands.gc]
            [onyx.log.commands.backpressure-on]
            [onyx.log.commands.backpressure-off]
            [onyx.log.commands.peer-replica-view]
            [onyx.log.commands.compact-bookkeeper-log-ids]
            [onyx.log.commands.assign-bookkeeper-log-id]
            [onyx.log.commands.deleted-bookkeeper-log-ids]
            [onyx.scheduling.greedy-job-scheduler]
            [onyx.scheduling.balanced-job-scheduler]
            [onyx.scheduling.percentage-job-scheduler]
            [onyx.scheduling.balanced-task-scheduler]
            [onyx.scheduling.percentage-task-scheduler]
            [onyx.windowing.units]
            [onyx.windowing.window-extensions]
            [onyx.windowing.aggregation]
            [onyx.triggers.triggers-api]
            [onyx.triggers.timer]
            [onyx.triggers.segment]
            [onyx.triggers.punctuation]
            [onyx.triggers.watermark]
            [onyx.triggers.percentile-watermark]
            [onyx.plugin.core-async]
            [clojure.core.async :refer [chan close!]]
            [onyx.extensions :as extensions]))

(def development-components [:monitoring :logging-config :log :bookkeeper])

(def client-components [:monitoring :log :messaging-require])

(def peer-components
  [:monitoring :log :messaging-require :messenger :acking-daemon :virtual-peer])

(def peer-group-components [:logging-config :messaging-require :messaging-group])

(def task-components
  [:task-lifecycle :register-messenger-peer :messenger-buffer :backpressure-poll])

(defn rethrow-component [f]
  (try
    (f)
    (catch Throwable e
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

(defrecord OnyxPeerGroup []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this peer-group-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this peer-group-components))))

(defn messenger-ctor [{:keys [config] :as peer-group}]
  (let [rets ((messenger config) peer-group)]
    (when-not rets
      (throw (ex-info "Could not find Messaging implementation" {:impl (:onyx.messaging/impl config)})))
    rets))

(defn messaging-require-ctor [config]
  (try
    (messaging-require config)
    :loaded
    (catch Throwable e
      (throw (ex-info "Could not find Messaging implementation" {:impl (:onyx.messaging/impl config)})))))

(defn messaging-peer-group-ctor [config]
  (let [rets ((messaging-peer-group config) config)]
    (when-not rets
      (throw (ex-info "Could not find Messaging implementation" {:impl (:onyx.messaging/impl config)})))
    rets))

(defn onyx-development-env
  ([peer-config]
     (onyx-development-env peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
     (map->OnyxDevelopmentEnv
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :logging-config (logging-config/logging-configuration peer-config)
       :bookkeeper (component/using (new-bookie peer-config) [:log])
       :log (component/using (zookeeper peer-config) [:monitoring :logging-config])})))

(defn onyx-client
  ([peer-config]
     (onyx-client peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
     (map->OnyxClient
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :messaging-require (messaging-require-ctor peer-config)
       :log (component/using (zookeeper peer-config) [:monitoring])})))

(defrecord RegisterMessengerPeer [messenger peer-site]
  component/Lifecycle
  (start [component]
    (extensions/register-task-peer messenger peer-site (:messenger-buffer component))
    component)
  (stop [component]
    (extensions/unregister-task-peer messenger peer-site)
    component))

(defrecord OnyxTask [peer-site peer-state task-state]
  component/Lifecycle
  (start [component]
    (rethrow-component
      #(component/start-system component task-components)))
  (stop [component]
    (rethrow-component
      #(component/stop-system component task-components))))

(defn onyx-task
  [peer-state task-state]
  (map->OnyxTask
    {:peer-state peer-state
     :task-state task-state
     :task-lifecycle (component/using (task-lifecycle peer-state task-state) [:messenger-buffer :register-messenger-peer])
     :backpressure-poll (component/using (backpressure-poll peer-state) [:messenger-buffer])
     :register-messenger-peer (component/using (map->RegisterMessengerPeer {:messenger (:messenger peer-state) 
                                                                       :peer-site (:peer-site task-state)}) 
                                          [:messenger-buffer])
     :messenger-buffer (buffer/messenger-buffer (:opts peer-state))}))

(defn onyx-peer
  ([peer-group]
     (onyx-peer peer-group {:monitoring :no-op}))
  ([{:keys [config] :as peer-group} monitoring-config]
     (map->OnyxPeer
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :messaging-require (messaging-require-ctor config)
       :log (component/using (zookeeper config) [:monitoring])
       :acking-daemon (component/using (acking-daemon config) [:monitoring :log])
       :messenger (component/using (messenger-ctor peer-group) [:monitoring :messaging-require :acking-daemon])
       :virtual-peer (component/using (virtual-peer config onyx-task) [:monitoring :log :acking-daemon :messenger])})))

(defn onyx-peer-group
  [config]
  (map->OnyxPeerGroup
   {:config config
    :logging-config (logging-config/logging-configuration config)
    :messaging-require (messaging-require-ctor config)
    :messaging-group (component/using (messaging-peer-group-ctor config) [:messaging-require :logging-config])}))
