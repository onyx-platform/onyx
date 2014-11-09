(ns ^:no-doc onyx.logging-configuration
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info] :as timbre]
            [com.postspectacular.rotor :as r]))

(defrecord LoggingConfiguration [id file config]
  component/Lifecycle

  (start [component]
    (if config
      (timbre/set-config! [] config)
      (do
        (timbre/set-config! [:appenders :standard-out :enabled?] false)
        (timbre/set-config! [:appenders :spit :enabled?] false)
        (timbre/set-config! [:shared-appender-config :spit-filename] file)
        (timbre/set-config!
         [:appenders :rotor]
         {:doc "Writes to to (:path (:rotor :shared-appender-config)) file
         and creates optional backups."
          :min-level :info
          :enabled? true
          :async? false
          :max-message-per-msecs nil
          :fn r/append})
        (timbre/set-config!
         [:shared-appender-config :rotor]
         {:path file :max-size (* 512 10240) :backlog 5})))

    (info "Starting Logging Configuration")
    component)

  (stop [component]
    (info "Stopping Logging Configuration")
    component))

(defn logging-configuration [id {:keys [onyx.log/file onyx.log/config]}]
  (map->LoggingConfiguration {:id id :file (or file "onyx.log") :config config}))

