(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [>!!]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [org.httpkit.client :as client]
              [taoensso.timbre :as timbre]
              [onyx.extensions :as extensions])
    (:import [java.nio ByteBuffer]))

(defn app [inbound-ch request]
  (>!! inbound-ch (:body request))
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body ""})

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [ch (:inbound-ch (:messenger-buffer component))
          server (server/run-server (partial app ch) {:port 0 :thread 1})]
      (assoc component :server server :port (:local-port (meta server)))))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit")

    ((:server component))
    component))

(defn http-kit [opts]
  (map->HttpKit {:opts opts}))

(defmethod extensions/receive-messages HttpKit
  [messenger event])

(defmethod extensions/send-messages HttpKit
  [messenger event]
  (doseq [c (map :compressed (:onyx.core/compressed event))]
    (prn c)
    (client/post "http://127.0.0.1:8081" {:body (ByteBuffer/wrap c)}))
  {})

