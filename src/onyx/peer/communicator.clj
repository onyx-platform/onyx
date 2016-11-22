(ns onyx.peer.communicator
  (:require [clojure.core.async :refer [go-loop <! >! >!! <!! alts!! promise-chan close! chan thread poll!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal trace]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.extensions :as extensions]
            [onyx.peer.log-version]
            [onyx.static.default-vals :refer [arg-or-default]]))

; Responsibilities
; - hold channels
; Useable to make duplex comunication between components
; (without the restrictions od direct acyclic nature of components system)
; CompA -> chan1 -> CompB
; CompB <- chan2 <- CompB
(defrecord Channels []
  component/Lifecycle
  (start [component]
    (info "Starting Channels")
    (let [supervisor-ch (chan 100)]

      (assoc component
        :supervisor-ch supervisor-ch)))

  (stop [{:keys [supervisor-ch] :as component}]
    (info "Stopping Channels")
    (when supervisor-ch (close! supervisor-ch))
    (assoc component :supervisor-ch nil)))

(defn new-channels []
  (map->Channels {}))

; Responsibilities
; - react after failures notifications
(defrecord Supervisor []
  component/Lifecycle
  (start [{:keys [channels] :as component}]
    (info "Starting Supervisor")
    (let [supervisor-ch (-> channels :supervisor-ch) ; notifications about failures
          handle-faulires (go-loop []
                                   (when-let [failure (<! supervisor-ch)]
                                     (println "Will handle failure:" failure)))
          ]
      (assoc component
        :supervisor-ch   supervisor-ch
        :handle-faulires handle-faulires)))

  (stop [{:keys [supervisor-ch handle-faulires] :as component}]
    (info "Stopping Supervisor")
    (when supervisor-ch   (close! supervisor-ch))
    (when handle-faulires (close! handle-faulires))
    (assoc component :supervisor-ch   nil
                     :handle-faulires nil)))

(defn new-supervisor []
  (map->Supervisor {}))


(defn outbox-loop [log outbox-ch group-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
       (trace "Log Writer: wrote - " entry)
       (extensions/write-log-entry log entry)
       (catch Throwable e
         (warn e "Replica services couldn't write to ZooKeeper.")
         (>!! group-ch [:restart-peer-group])))
      (recur))))

(defrecord LogWriter [peer-config group-ch]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Log Writer")
    (let [outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity peer-config))
          outbox-loop-thread (thread (outbox-loop log outbox-ch group-ch))]
      (assoc component
             :outbox-ch outbox-ch
             :outbox-loop-thread outbox-loop-thread)))

  (stop [{:keys [outbox-loop-thread outbox-ch] :as component}]
    (taoensso.timbre/info "Stopping Log Writer")
    (close! outbox-ch)
    ;; Wait for outbox to drain
    (<!! outbox-loop-thread)
    component))

(defn log-writer [peer-config group-ch]
  (->LogWriter peer-config group-ch))

(defrecord ReplicaSubscription [peer-config]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Replica Subscription")
    ;; Race to write the job scheduler and messaging to durable storage so that
    ;; non-peers subscribers can discover which properties to use.
    ;; Only one writer will succeed, and only one needs to.

    (extensions/write-chunk log 
                            :log-parameters 
                            {:job-scheduler (:onyx.peer/job-scheduler peer-config)
                             :messaging (select-keys peer-config [:onyx.messaging/impl])
                             :log-version onyx.peer.log-version/version} 
                            nil)

    (let [group-id (java.util.UUID/randomUUID)
          inbox-ch (chan (arg-or-default :onyx.peer/inbox-capacity peer-config))
          origin (extensions/subscribe-to-log log inbox-ch)]
      (assoc component
             :group-id group-id
             :inbox-ch inbox-ch
             :replica-origin origin)))

  (stop [component]
    (taoensso.timbre/info "Stopping Replica Subscription")
    (close! (:inbox-ch component))
    component))

(defn replica-subscription [peer-config]
  (->ReplicaSubscription peer-config))

(defrecord OnyxComm []
  component/Lifecycle
  (start [this]
    (info "Starting OnyxComm")
    (component/start-system this [:channels :supervisor :log :logging-config :replica-subscription :log-writer]))
  (stop [this]
    (info "Stopping OnyxComm")
    (component/stop-system this [:channels :supervisor :log :logging-config :replica-subscription :log-writer])))

(defn onyx-comm
  [peer-config group-ch monitoring]
   (map->OnyxComm
    {:channels   (component/using (new-channels) [])
     :supervisor (component/using (new-supervisor) [:channels])
     :config peer-config
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring monitoring
     :log (component/using (zookeeper peer-config) [:monitoring :channels :supervisor])
     :replica-subscription (component/using (replica-subscription peer-config) [:log])
     :log-writer (component/using (log-writer peer-config group-ch) [:log])}))
