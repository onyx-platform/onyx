(ns ^:no-doc onyx.messaging.acking-daemon
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]))

(defrecord AckingDaemon []
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Acking Daemon")
    (assoc component :ack-state (atom {})))

  (stop [component]
    (taoensso.timbre/info "Stopping Acking Daemon")
    (assoc component :ack-state nil)))

(defn acking-daemon [config]
  (map->AckingDaemon {}))

