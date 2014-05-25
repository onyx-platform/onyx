(ns onyx.logging-configuration
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info] :as timbre]))

(defrecord LoggingConfiguration [file]
  component/Lifecycle

  (start [component]
    (timbre/set-config! [:appenders :standard-out :enabled?] false)
    (timbre/set-config! [:appenders :spit :enabled?] true)
    (timbre/set-config! [:shared-appender-config :spit-filename] file)

    (info "Starting Logging Configuration")

    component)

  (stop [component]
    (info "Stopping Logging Configuration")
    component))

(defn logging-configuration [file]
  (map->LoggingConfiguration {:file (or file "onyx.log")}))

