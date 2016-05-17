(ns onyx.system
  (:require [clojure.core.async :refer [chan close!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.static.onyx-component :refer [map->ComponentSystem]]
            [onyx.peer.virtual-peer :refer [virtual-peer virtual-peers]]
            [onyx.peer.replica :refer [replica-subscription replica-controller]]
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

(defrecord RegisterMessengerPeer [messenger peer-site]
  component/Lifecycle
  (start [component]
    (extensions/register-task-peer messenger peer-site (:messenger-buffer component))
    component)
  (stop [component]
    (extensions/unregister-task-peer messenger peer-site)
    component))

(defn onyx-development-env
  ([peer-config]
     (onyx-development-env peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
     (map->ComponentSystem
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :logging-config (logging-config/logging-configuration peer-config)
       :bookkeeper (component/using (multi-bookie-server peer-config) [:log])
       :log (component/using (zookeeper peer-config) [:monitoring :logging-config])})))

(defn onyx-client
  ([peer-config]
     (onyx-client peer-config {:monitoring :no-op}))
  ([peer-config monitoring-config]
     (map->ComponentSystem
      {:monitoring (extensions/monitoring-agent monitoring-config)
       :log (component/using (zookeeper peer-config) [:monitoring])})))

(defn onyx-peer-group
  ([restart-ch peer-config]
   (onyx-peer-group restart-ch peer-config {:monitoring :no-op}))
  ([restart-ch peer-config monitoring-config]
   (map->ComponentSystem
    {:config peer-config
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring (component/using (extensions/monitoring-agent monitoring-config) [:logging-config])
     :log (component/using (zookeeper peer-config) [:monitoring])
     :messaging-group (component/using (am/aeron-peer-group peer-config) [:log :logging-config])
     :replica-subscription (component/using (replica-subscription peer-config) [:log])
     :virtual-peers (component/using (virtual-peers peer-config) [:log :replica-subscription])
     :replica-controller (component/using (replica-controller peer-config restart-ch) [:log :monitoring :replica-subscription])})))

(defn onyx-task
  [peer-state task-state]
  (map->ComponentSystem
   {:logging-config (:logging-config peer-state)
    :peer-state peer-state
    :task-state task-state
    :task-information (component/using (new-task-information peer-state task-state) [])
    :task-monitoring (component/using (:monitoring peer-state) [:task-information])
    :task-lifecycle (component/using (task-lifecycle peer-state task-state)
                                     [:task-information 
                                      :messenger-buffer 
                                      :task-monitoring
                                      :register-messenger-peer])
    :backpressure-poll (component/using (backpressure-poll peer-state)
                                        [:messenger-buffer :task-monitoring])
    :register-messenger-peer (component/using
                              (map->RegisterMessengerPeer 
                               {:messenger (:messenger peer-state) 
                                :peer-site (:peer-site task-state)}) [:messenger-buffer])
    :messenger-buffer (buffer/messenger-buffer (:opts peer-state))}))

(defn onyx-vpeer-system
  [{:keys [config] :as peer-group}]
  (map->ComponentSystem
   {:acking-daemon (acking-daemon config)
    :messenger (component/using (am/aeron-messenger peer-group) [:acking-daemon])
    :virtual-peer (component/using (virtual-peer config peer-group onyx-task) [:acking-daemon :messenger])}))
