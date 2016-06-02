(ns onyx.peer.communicator
  (:require [clojure.core.async :refer [>!! <!! alts!! promise-chan close! chan thread poll!]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info error warn fatal]]
            [onyx.static.logging-configuration :as logging-config]
            [onyx.log.zookeeper :refer [zookeeper]]
            [onyx.extensions :as extensions]
            [onyx.static.default-vals :refer [arg-or-default]]))

(defn outbox-loop [log outbox-ch command-ch]
  (loop []
    (when-let [entry (<!! outbox-ch)]
      (try
        (extensions/write-log-entry log entry)
        (catch Throwable e
          (warn e "Replica services couldn't write to ZooKeeper.")
          (>!! command-ch [:restart-peer-group])))
      (recur))))

(defrecord LogWriter [peer-config command-ch]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Log Writer")
    (let [outbox-ch (chan (arg-or-default :onyx.peer/outbox-capacity peer-config))
          outbox-loop-thread (thread (outbox-loop log outbox-ch command-ch))]
      (assoc component
             :outbox-ch outbox-ch
             :outbox-loop-thread outbox-loop-thread)))

  (stop [component]
    (taoensso.timbre/info "Stopping Log Writer")
    (close! (:outbox-ch component))
    ;; Unblock any writers
    (while (poll! (:outbox-ch component)))
    component))

(defn log-writer [peer-config command-ch]
  (->LogWriter peer-config command-ch))

(defrecord ReplicaSubscription [peer-config]
  component/Lifecycle

  (start [{:keys [log] :as component}]
    (taoensso.timbre/info "Starting Replica Subscription")
    ;; Race to write the job scheduler and messaging to durable storage so that
    ;; non-peers subscribers can discover which properties to use.
    ;; Only one writer will succeed, and only one needs to.
    (extensions/write-chunk log :job-scheduler {:job-scheduler (:onyx.peer/job-scheduler peer-config)} nil)
    (extensions/write-chunk log :messaging {:messaging (select-keys peer-config [:onyx.messaging/impl])} nil)

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
    (component/start-system this [:log :logging-config :replica-subscription :log-writer]))
  (stop [this]
    (component/stop-system this [:log :logging-config :replica-subscription :log-writer])))

(defn onyx-comm
  ([peer-config command-ch]
   (onyx-comm peer-config command-ch {:monitoring :no-op}))
  ([peer-config command-ch monitoring-config]
   (map->OnyxComm
    {:config peer-config
     :logging-config (logging-config/logging-configuration peer-config)
     :monitoring (component/using (extensions/monitoring-agent monitoring-config) [:logging-config])
     :log (component/using (zookeeper peer-config) [:monitoring])
     :replica-subscription (component/using (replica-subscription peer-config) [:log])
     :log-writer (component/using (log-writer peer-config command-ch) [:log])})))
