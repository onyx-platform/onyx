(ns ^:no-doc onyx.messaging.http-kit
    (:require [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [org.httpkit.server :as http]))

(defn app [request]
  (println (format "Dropping %s on the floor" request))
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body ""})

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [server (http/run-server app {:port 0 :thread 1})]
      (assoc component :server server)))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit")

    ((:server component))
    component))

(defn http-kit [opts]
  (map->HttpKit {:opts opts}))

