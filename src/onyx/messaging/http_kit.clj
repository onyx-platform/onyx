(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [>!!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [org.httpkit.server :as http]))

(defn app [inbound-ch request]
  (>!! inbound-ch (:body request))
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body ""})

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [ch (:inbound-ch (:messaging-buffer component))
          server (http/run-server (partial app ch) {:port 0 :thread 1})]
      (assoc component :server server)))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit")

    ((:server component))
    component))

(defn http-kit [opts]
  (map->HttpKit {:opts opts}))

