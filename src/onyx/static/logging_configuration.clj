(ns ^:no-doc onyx.static.logging-configuration
  (:require [com.stuartsierra.component :as component]
            [io.aviso.exception :as pretty]
            [taoensso.timbre :refer [info] :as timbre]
            [taoensso.timbre.appenders.3rd-party.rotor :as rotor]))

(def MAX-LOG-SIZE (* 512 102400))
(def MAX-LOG-FILES 5)

;; temporary workaround to prevent timbre from crashing under some circumstances
;; e.g. java.lang.IllegalArgumentException: Multiple methods in multimethod 'exception-dispatch' match dispatch value: class schema.core.Either 
;; -> interface clojure.lang.IPersistentMap and interface clojure.lang.IRecord, and neither is preferred]
(defmethod pretty/exception-dispatch clojure.lang.IPersistentMap [m]
  (prn m))

(defrecord LoggingConfiguration [file config]
  component/Lifecycle

  (start [component]
    (if config
      (timbre/merge-config! config)
      (let [rotor-appender (rotor/rotor-appender
                             {:path file
                              :max-size MAX-LOG-SIZE
                              :backlog MAX-LOG-FILES})
            rotor-appender (assoc rotor-appender :min-level :info)]
        (timbre/merge-config!
          {:appenders
           {:println
            {:min-level :error
             :enabled? true}
            :rotor rotor-appender}})))

    (info "Starting Logging Configuration")
    component)

  (stop [component]
    (info "Stopping Logging Configuration")
    component))

(defn logging-configuration [{:keys [onyx.log/file onyx.log/config]}]
  (map->LoggingConfiguration {:file (or file "onyx.log") :config config}))

(defmethod clojure.core/print-method LoggingConfiguration
  [system ^java.io.Writer writer]
  (.write writer "#<Logging Configuration>"))
