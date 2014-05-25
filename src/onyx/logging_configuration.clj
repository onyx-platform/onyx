(ns onyx.logging-configuration
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info] :as timbre]))

(defrecord LoggingConfiguration []
  component/Lifecycle

  (start [component]
    (info "Starting Logging Configuration")

    (timbre/set-config!
     [:appenders :standard-out]
     {:min-level nil :enabled? true :async? false :rate-limit nil
      :fn (fn [{:keys [timestamp error? output message]}]
            (binding [*out* (if error? *err* *out*)]
              (timbre/str-println timestamp message)))}))

  (stop [component]
    (info "Stopping Logging Configuration")
    component))

(defn logging-configuration []
  (map->LoggingConfiguration {}))

