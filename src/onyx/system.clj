(ns onyx.system
  (:require [clojure.core.async :refer [chan close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.peer.task-lifecycle :refer [task-lifecycle new-task-information]]
            [onyx.peer.backpressure-poll :refer [backpressure-poll]]
            [onyx.messaging.acking-daemon :refer [acking-daemon]]
            [onyx.messaging.aeron :as am]
            [onyx.messaging.messenger-buffer :as buffer]
            [onyx.monitoring.no-op-monitoring]
            [onyx.monitoring.custom-monitoring]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.state.bookkeeper :refer [multi-bookie-server]]
            [onyx.state.log.bookkeeper]
            [onyx.state.log.none]
            [onyx.state.filter.set]
            [onyx.state.filter.rocksdb]
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
            [onyx.scheduling.colocated-task-scheduler]
            [onyx.windowing.units]
            [onyx.windowing.window-extensions]
            [onyx.windowing.aggregation]
            [onyx.triggers]
            [onyx.refinements]
            [onyx.compression.nippy]
            [onyx.plugin.core-async]
            [onyx.extensions :as extensions]
            [onyx.interop]))

(defn rethrow-component [f]
  (try
    (f)
    (catch Throwable e
      (fatal e)
      (throw (.getCause e)))))

(def development-components [:monitoring :logging-config :log :bookkeeper])

(defrecord OnyxDevelopmentEnv []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this development-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this development-components))))

(def client-components [:monitoring :log])

(defrecord OnyxClient []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this client-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this client-components))))

(def peer-components
  [:monitoring :log :messenger :acking-daemon :virtual-peer])

(defrecord OnyxPeer []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this peer-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this peer-components))))

(def peer-group-components [:logging-config :messaging-group])

(defrecord OnyxPeerGroup []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this peer-group-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this peer-group-components))))

(defn onyx-development-env
  ([peer-config]
     (onyx-development-env peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
     (map->OnyxDevelopmentEnv
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :logging-config (logging-config/logging-configuration peer-config)
       :bookkeeper (component/using (multi-bookie-server peer-config) [:log])
       :log (component/using (zookeeper peer-config) [:monitoring :logging-config])})))

(defn onyx-client
  ([peer-config]
     (onyx-client peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
     (map->OnyxClient
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :log (component/using (zookeeper peer-config) [:monitoring])})))

(defrecord RegisterMessengerPeer [messenger peer-site]
  component/Lifecycle
  (start [component]
    (extensions/register-task-peer messenger peer-site (:messenger-buffer component))
    component)
  (stop [component]
    (extensions/unregister-task-peer messenger peer-site)
    component))

(def task-components
  [:task-lifecycle :register-messenger-peer :messenger-buffer :backpressure-poll :task-information :task-monitoring])

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
     :task-information (component/using (new-task-information peer-state task-state) [])
     :task-monitoring (component/using (:monitoring peer-state) [:task-information])
     :task-lifecycle (component/using (task-lifecycle peer-state task-state) [:task-information 
                                                                              :messenger-buffer 
                                                                              :task-monitoring
                                                                              :register-messenger-peer])
     :backpressure-poll (component/using (backpressure-poll peer-state) [:messenger-buffer :task-monitoring])
     :register-messenger-peer (component/using (map->RegisterMessengerPeer 
                                                 {:messenger (:messenger peer-state) 
                                                  :peer-site (:peer-site task-state)}) [:messenger-buffer])
     :messenger-buffer (buffer/messenger-buffer (:opts peer-state))}))

(defn onyx-peer
  ([peer-group]
     (onyx-peer peer-group {:monitoring :no-op}))
  ([{:keys [config] :as peer-group} monitoring-config]
     (map->OnyxPeer
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :log (component/using (zookeeper config) [:monitoring])
       :acking-daemon (component/using (acking-daemon config) [:monitoring :log])
       :messenger (component/using (am/aeron-messenger peer-group) [:monitoring :acking-daemon])
       :virtual-peer (component/using (virtual-peer config onyx-task) [:monitoring :log :acking-daemon :messenger])})))

(defn onyx-peer-group
  [config]
  (map->OnyxPeerGroup
   {:config config
    :logging-config (logging-config/logging-configuration config)
    :messaging-group (component/using (am/aeron-peer-group config) [:logging-config])}))

(defmethod clojure.core/print-method OnyxPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Peer>"))
