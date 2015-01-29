(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [>!! alts!! timeout]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [org.httpkit.client :as client]
              [taoensso.timbre :as timbre]
              [onyx.extensions :as extensions])
    (:import [java.nio ByteBuffer]))

(defn app [inbound-ch request]
  (>!! inbound-ch (.bytes (:body request)))
  {:status 200
   :headers {"Content-Type" "text/html"}
   :body ""})

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [ch (:inbound-ch (:messenger-buffer component))
          ip "0.0.0.0"
          server (server/run-server (partial app ch) {:ip ip :port 0 :thread 1})]
      (assoc component :server server :ip ip :port (:local-port (meta server)))))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit")

    ((:server component))
    component))

(defn http-kit [opts]
  (map->HttpKit {:opts opts}))

(defmethod extensions/peer-site HttpKit
  [messenger]
  {:url (format "http://%s:%s" (:ip messenger) (:port messenger))})

(defmethod extensions/receive-messages HttpKit
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 1000)
        ch (:inbound-ch (:onyx.core/messenger-buffer event))]
    (filter
     identity
     (map (fn [_] (first (alts!! [ch (timeout ms)])))
          (range (:onyx/batch-size task-map))))))

(defmethod extensions/send-messages HttpKit
  [messenger event peer-site]
  (doseq [c (map :compressed (:onyx.core/compressed event))]
    (client/post (:url peer-site) {:body (ByteBuffer/wrap c)}))
  {})

