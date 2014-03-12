(ns onyx.logger
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :as timbre]))

(defrecord Logger []
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Logger")

    (timbre/set-config!
     [:appenders :standard-out]
     {:min-level nil :enabled? true :async? false :rate-limit nil
      :fn (fn [{:keys [timestamp error? output message]}]
            (binding [*out* (if error? *err* *out*)]
              (timbre/str-println timestamp message)))})

    component)

  (stop [component]
    (taoensso.timbre/info "Stopping Virtual Peer")

    component))

(defn logger []
  (map->Logger {}))

