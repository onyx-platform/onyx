(ns onyx.system
  (:require [clojure.core.async :refer [go-loop pub sub go >! <! chan >!! <!! close! thread alts!! offer!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.peer.virtual-peer :refer [virtual-peer]]
            [onyx.peer.task-lifecycle :refer [task-lifecycle new-task-information]]
            [onyx.peer.backpressure-poll :refer [backpressure-poll]]
            [onyx.peer.peer-group-manager :as pgm]
            [onyx.peer.communicator :as comm]
            [onyx.messaging.acking-daemon :refer [acking-daemon]]
            [onyx.messaging.aeron :as am]
            [onyx.messaging.messenger-buffer :as buffer]
            [onyx.monitoring.no-op-monitoring]
            [onyx.monitoring.custom-monitoring]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.query :as qs]
            [onyx.static.validation :as validator]
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
            [onyx.log.commands.group-leave-cluster]
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
            [onyx.log.commands.add-virtual-peer]
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
            [onyx.interop]
            [peripheral.component
             [attach :refer [attach]]
             [state :refer [running?]]]
            [peripheral.component :refer [defcomponent]]
            [peripheral.system-plus :refer :all]
            [clojure.walk :as w]))

(def development-components [:channels :supervisor :monitoring :logging-config :log :log-supervisor :bookkeeper])

(def peer-group-components [:logging-config :monitoring :query-server :messaging-group :peer-group-manager])

(def peer-components [:messenger :acking-daemon :virtual-peer])

(def task-components [:task-lifecycle :register-messenger-peer
                      :messenger-buffer :backpressure-poll
                      :task-information :task-monitoring])

(def client-components [:monitoring :log])

(defrecord RegisterMessengerPeer [messenger peer-site]
  component/Lifecycle
  (start [component]
    (extensions/register-task-peer messenger peer-site (:messenger-buffer component))
    component)
  (stop [component]
    (extensions/unregister-task-peer messenger peer-site)
    component))

(defn rethrow-component [f]
  (try
    (f)
    (catch Throwable e
      (fatal e)
      (throw (.getCause e)))))

;(defrecord OnyxDevelopmentEnv []
;  component/Lifecycle
;  (start [this]
;    (info "Starting OnyxDevelopmentEnv system")
;    (rethrow-component
;     #(component/start-system this development-components)))
;  (stop [this]
;    (info "Stopping OnyxDevelopmentEnv system")
;    (rethrow-component
;     #(component/stop-system this development-components))))

(defsystem+ OnyxDevelopmentEnv []
            :logging-config []
            :channels       [:logging-config]
            :monitoring     [:logging-config]
            :supervisor     [:logging-config :channels]
            :log-supervisor [:logging-config :channels])

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

(defrecord OnyxTask [peer-site peer-state task-state]
  component/Lifecycle
  (start [component]
    (rethrow-component
     #(component/start-system component task-components)))
  (stop [component]
    (rethrow-component
     #(component/stop-system component task-components))))


;; Supervisor to control components that use ZooKeeper
(def zks-children (atom {}))
(defcomponent ZooKeeperSupervisor [peer-config channels]
              :on/start (do (info    "Starting ZooKeeperSupervisor")
                            (println "Starting ZooKeeperSupervisor"))
              :on/stop  (do (info    "Stopping ZooKeeperSupervisor")
                            (println "Stopping ZooKeeperSupervisor"))
              ;; fields
              :bus        (-> channels :bus)
              :bus-ch     (chan 1)
                          (fn [v] (when v (close! v))) ;on shutdown
              :s1         (sub bus :shutdown bus-ch)


              :peripheral/started
              (fn [{:keys [bus-ch] :as this}]
                  ; 1. create child components
                  ; 2. subscribe to messages from supervisor
                  (let [log       (component/start (zookeeper peer-config channels))
                        bookeeper (component/start (multi-bookie-server peer-config log))
                        _         (swap! zks-children assoc :bookkeeper bookeeper
                                                            :log        log)

                        this' (->> (go-loop []
                                            (when-let [b (<! bus-ch)]
                                               (do (println "Handle from bus:" b)
                                                   (println "Supervisor children stopping ....")
                                                   (let [{:keys [log bookkeeper]} @zks-children]
                                                     (swap! zks-children assoc :bookkeeper nil
                                                                               :log        nil)
                                                     (when bookkeeper (component/stop bookkeeper))
                                                     (when log        (component/stop log)))

                                                   (println "Supervisor children stopped")
                                                   (clojure.pprint/pprint @zks-children))))
                                    (assoc this :handle-bus))
                        ]
                    (println "Started ZooKeeperSupervisor")
                    ;(clojure.pprint/pprint this')
                    this'))

              :peripheral/stopped
              (fn [{:keys [handle-bus] :as this}]
                   (println "Stopped ZooKeeperSupervisor ...")
                   ;(clojure.pprint/pprint this)
                   (when handle-bus (close! handle-bus))
                   ;(clojure.pprint/pprint @zks-children)
                   (let [{:keys [log bookkeeper]} @zks-children]
                     (swap! zks-children assoc :bookkeeper nil
                                               :log        nil)
                     (when bookkeeper (component/stop bookkeeper))
                     (when log        (component/stop log)))

                  this)
              )

(defn new-zookeeper-supervisor [peer-config]
  (map->ZooKeeperSupervisor {:peer-config peer-config}))

;(defmethod clojure.core/print-method ZooKeeperSupervisor
;  [system ^java.io.Writer writer]
;  (.write writer "#<ZooKeeperSupervisor Component>"))


(defn onyx-development-env
  [{:keys [monitoring-config]
    :or {monitoring-config {:monitoring :no-op}}
    :as peer-config}]
  ;(map->OnyxDevelopmentEnv
  ;  {:channels   (component/using (comm/new-channels) [])
  ;   :supervisor (component/using (comm/new-supervisor) [:channels])
  ;   :monitoring (extensions/monitoring-agent monitoring-config)
  ;   :logging-config (logging-config/logging-configuration peer-config)
  ;   :bookkeeper (component/using (multi-bookie-server peer-config) [:log])
  ;   :log            (component/using (zookeeper peer-config) [:channels :supervisor :monitoring :logging-config])
  ;   :log-supervisor (component/using (new-zookeeper-supervisor) [:channels :log])})
  (-> (map->OnyxDevelopmentEnv
        {:channels       (comm/new-channels)
         :supervisor     (comm/new-supervisor)
         :monitoring     (extensions/monitoring-agent monitoring-config)
         :logging-config (logging-config/logging-configuration peer-config)
         :log-supervisor (new-zookeeper-supervisor peer-config)
         ;:bookkeeper     (multi-bookie-server peer-config)
         ;:log            (zookeeper peer-config)
         ;:log-supervisor (new-zookeeper-supervisor)
         ;:log-supervisor (-> (new-zookeeper-supervisor)
         ;                    (attach :log (zookeeper peer-config) {:channels :channels})
         ;                    )

         })
      component/start

      ;(attach :log        (zookeeper peer-config)           [:channels])
      ;(attach :bookkeeper (multi-bookie-server peer-config) [:log])

      ;(attach :log-supervisor
      ;        (-> (new-zookeeper-supervisor)
      ;            (attach :log        (zookeeper peer-config)           [:channels])
      ;            (attach :bookkeeper (multi-bookie-server peer-config) {:log :log}))
      ;        {:channels :channels})

      ))

(defn onyx-client
  [{:keys [monitoring-config]
    :or {monitoring-config {:monitoring :no-op}}
    :as peer-client-config}]
  (validator/validate-peer-client-config peer-client-config)
  (map->OnyxClient
    {:monitoring (extensions/monitoring-agent monitoring-config)
     :log (component/using (zookeeper peer-client-config nil) [:monitoring])}))

(defn onyx-task
  [peer-state task-state]
  (map->OnyxTask
   {:logging-config (:logging-config peer-state)
    :peer-state peer-state
    :task-state task-state
    :task-information (new-task-information peer-state task-state)
    :messenger-buffer (buffer/messenger-buffer (:opts peer-state))
    :task-monitoring (component/using
                      (:monitoring peer-state)
                      [:logging-config :task-information])
    :backpressure-poll (component/using
                        (backpressure-poll peer-state)
                        [:messenger-buffer :task-monitoring])
    :register-messenger-peer
    (component/using (map->RegisterMessengerPeer
                      {:messenger (:messenger peer-state)
                       :peer-site (:peer-site task-state)}) [:messenger-buffer])
    :task-lifecycle (component/using
                     (task-lifecycle peer-state task-state)
                     [:task-information
                      :messenger-buffer
                      :task-monitoring
                      :register-messenger-peer])}))

(defn onyx-vpeer-system
  [group-ch outbox-ch peer-config messaging-group monitoring log group-id vpeer-id]
   (map->OnyxPeer
    {:group-id group-id
     :messaging-group messaging-group
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring monitoring 
     :acking-daemon (component/using
                     (acking-daemon peer-config)
                     [:monitoring])
     :messenger (component/using
                 (am/aeron-messenger peer-config messaging-group)
                 [:monitoring :acking-daemon])
     :virtual-peer (component/using
                    (virtual-peer group-ch outbox-ch log peer-config onyx-task vpeer-id)
                    [:group-id :messaging-group :monitoring :acking-daemon
                     :messenger :logging-config])}))

(defn onyx-peer-group
  [{:keys [monitoring-config]
    :or {monitoring-config {:monitoring :no-op}}
    :as peer-config}]
  (map->OnyxPeerGroup
    {:config peer-config
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring (component/using (extensions/monitoring-agent monitoring-config) [:logging-config])
     :messaging-group (component/using (am/aeron-peer-group peer-config) [:logging-config])
     :query-server (component/using (qs/query-server peer-config) [:logging-config])
     :peer-group-manager (component/using (pgm/peer-group-manager peer-config onyx-vpeer-system)
                                          [:logging-config :monitoring :messaging-group :query-server])}))

(defmethod clojure.core/print-method OnyxPeer
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Peer>"))

(defmethod clojure.core/print-method OnyxPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Peer Group>"))

(defmethod clojure.core/print-method OnyxTask
  [system ^java.io.Writer writer]
  (.write writer "#<Onyx Task>"))
