(ns ^:no-doc onyx.static.logging-configuration
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info] :as timbre]
            [taoensso.timbre.appenders.rotor :as rotor]))

(defrecord LoggingConfiguration [file config]
  component/Lifecycle

  (start [component]
    (if config
      (timbre/merge-config! config)
      (do
        (timbre/set-config!
          [:appenders :rotor]
          {:min-level :info
           :enabled? true
           :async? false
           :max-message-per-msecs nil
           :fn rotor/appender-fn})
        (timbre/set-config!
          [:shared-appender-config :rotor]
          {:path file :max-size (* 512 10240) :backlog 5})
        (timbre/set-config! [:appenders :standard-out]
                            {:min-level :error
                             :enabled? true})
        (timbre/set-config! [:appenders :spit :enabled?] false)))

    (info "Starting Logging Configuration")
    component)

  (stop [component]
    (info "Stopping Logging Configuration")
    component))

(defn logging-configuration [{:keys [onyx.log/file onyx.log/config]}]
  (map->LoggingConfiguration {:file (or file "onyx.log") :config config}))

