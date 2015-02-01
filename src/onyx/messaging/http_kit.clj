(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [>!! alts!! timeout]]
              [clojure.data.fressian :as fressian]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [org.httpkit.client :as client]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.extensions :as extensions])
    (:import [java.nio ByteBuffer]))

(def acker-route "/ack")

(defn app [daemon inbound-ch request]
  (let [thawed (fressian/read (.bytes (:body request)))]
    (if (= (:uri request) acker-route)
      (acker/ack-message daemon
                         (:id thawed)
                         (:completion-id thawed)
                         (:ack-val thawed))
      (doseq [message thawed]
        (>!! inbound-ch message)))
    {:status 200
     :headers {"Content-Type" "text/plain"}}))

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [ch (:inbound-ch (:messenger-buffer component))
          daemon (:acking-daemon component)
          ip "0.0.0.0"
          server (server/run-server (partial app daemon ch) {:ip ip :port 0 :thread 1 :queue-size 1000})]
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
  (let [messages (:onyx.core/compressed event)
        compressed-batch (fressian/write messages)]
    (client/post (:url peer-site) {:body (ByteBuffer/wrap (.array compressed-batch))}))
  {})

(defmethod extensions/internal-ack-message HttpKit
  [messenger event message-id acker-id completion-id ack-val]
  (let [replica @(:onyx.core/replica event)
        url (:url (get-in replica [:peer-site acker-id]))
        route (format "%s%s" url acker-route)
        contents (fressian/write {:id message-id :completion-id completion-id :ack-val ack-val})]
    (client/post route {:body (ByteBuffer/wrap (.array contents))})))

(defmethod extensions/internal-complete-message HttpKit
  [messenger id])

