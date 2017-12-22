(ns onyx.system
  (:require [clojure.core.async :refer [chan close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.peer.task-lifecycle :refer [task-lifecycle new-task-information]]
            [onyx.monitoring.metrics-monitoring :as metrics-monitoring]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.aeron.messaging-group]
            [onyx.messaging.aeron.messenger]
            [onyx.peer.queryable-state-manager :as queryable-state]
            [onyx.peer.peer-group-manager :as pgm]
            [onyx.monitoring.no-op-monitoring]
            [onyx.monitoring.custom-monitoring]
            [onyx.peer.function]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.query :as qs]
            [onyx.static.validation :as validator]
            [onyx.state.lmdb]
            [onyx.state.memory]
            [onyx.log.commands.prepare-join-cluster]
            [onyx.log.commands.accept-join-cluster]
            [onyx.log.commands.abort-join-cluster]
            [onyx.log.commands.notify-join-cluster]
            [onyx.log.commands.set-replica]
            [onyx.log.commands.group-leave-cluster]
            [onyx.log.commands.leave-cluster]
            [onyx.log.commands.submit-job]
            [onyx.log.commands.kill-job]
            [onyx.log.commands.gc]
            [onyx.log.commands.add-virtual-peer]
            [onyx.log.commands.complete-job]
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
            [onyx.plugin.protocols]
            [onyx.plugin.core-async]
            [onyx.storage.s3]
            [onyx.extensions :as extensions]
            [onyx.checkpoint :as checkpoint]
            [onyx.interop]))

(def development-components [:monitoring :logging-config :log])

(def peer-group-components [:logging-config :monitoring :query-server :messenger-group :peer-group-manager :state-store-group])

(def client-components [:monitoring :log])

(def task-components
  [:task-lifecycle :task-information])

(def peer-components
  [:virtual-peer])

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

(defn onyx-development-env
  [peer-config]
  (map->OnyxDevelopmentEnv
   {:monitoring (extensions/monitoring-agent {:monitoring :no-op})
    :logging-config (logging-config/logging-configuration peer-config)
    :log (component/using (zookeeper peer-config) [:monitoring :logging-config])}))

(defrecord OnyxClient []
  component/Lifecycle
  (start [this]
    (rethrow-component
     #(component/start-system this client-components)))
  (stop [this]
    (rethrow-component
     #(component/stop-system this client-components))))

(defrecord OnyxTask [peer-site peer-state task-state]
  component/Lifecycle
  (start [component]
    (rethrow-component
     #(component/start-system component task-components)))
  (stop [component]
    (rethrow-component
     #(component/stop-system component task-components))))

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

(defn onyx-client
  [peer-client-config]
  (validator/validate-peer-client-config peer-client-config)
  (map->OnyxClient
   {:peer-config peer-client-config
    :monitoring (extensions/monitoring-agent {:monitoring :no-op})
    :log (component/using (zookeeper peer-client-config) [:monitoring])}))

(defn onyx-task
  [peer-state task-state]
  (map->OnyxTask
   {:logging-config (:logging-config peer-state)
    :peer-state peer-state
    :task-state task-state
    :task-information (new-task-information peer-state task-state)
    :task-lifecycle (component/using (task-lifecycle peer-state task-state) [:task-information])}))

(defn onyx-vpeer-system
  [group-ch outbox-ch peer-config messenger-group state-store-group monitoring log group-id vpeer-id]
   (map->OnyxPeer
    {:group-id group-id
     :messenger-group messenger-group
     :state-store-group state-store-group
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring monitoring 
     :virtual-peer (component/using
                    (virtual-peer group-ch outbox-ch log peer-config onyx-task vpeer-id)
                    [:group-id :messenger-group :monitoring :logging-config :state-store-group])}))

(defn onyx-peer-group
  [peer-config]
  (map->OnyxPeerGroup
   {:config peer-config
    :logging-config (logging-config/logging-configuration peer-config)
    :monitoring (component/using (metrics-monitoring/new-monitoring) [:logging-config])
    :state-store-group (component/using (queryable-state/new-state-store-group peer-config) [:logging-config]) 
    :messenger-group (component/using (m/build-messenger-group peer-config) [:logging-config])
    :query-server (component/using (qs/query-server peer-config) [:logging-config :state-store-group])
    :peer-group-manager (component/using (pgm/peer-group-manager peer-config onyx-vpeer-system) 
                                         [:logging-config :monitoring
                                          :state-store-group :messenger-group
                                          :query-server])}))

(defmethod clojure.core/print-method OnyxPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Peer>"))

(defmethod clojure.core/print-method OnyxPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Peer Group>"))

(defmethod clojure.core/print-method OnyxTask
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Task>"))
