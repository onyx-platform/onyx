(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close!]]
              [clojure.data.fressian :as fressian]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [org.httpkit.client :as client]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.extensions :as extensions])
    (:import [java.nio ByteBuffer]))

(def send-route "/send")

(def acker-route "/ack")

(def completion-route "/completion")

(defn app [daemon inbound-ch release-ch request]
  (let [thawed (fressian/read (.bytes (:body request)))
        uri (:uri request)]
    (cond (= uri send-route)
          (doseq [message thawed]
            (>!! inbound-ch message))

          (= uri acker-route)
          (acker/ack-message daemon
                             (:id thawed)
                             (:completion-id thawed)
                             (:ack-val thawed))

          (= uri completion-route)
          (>!! release-ch (:id thawed)))
    {:status 200
     :headers {"Content-Type" "text/plain"}}))

(defrecord HttpKit [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit")

    (let [ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 1000))
          daemon (:acking-daemon component)
          ip "0.0.0.0"
          server (server/run-server (partial app daemon ch release-ch) {:ip ip :port 0 :thread 1 :queue-size 1000})]
      (assoc component :server server :ip ip :port (:local-port (meta server)) :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit")

    (close! (:release-ch component))
    ((:server component))
    (assoc component :release-ch nil)))

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
        url (:url peer-site)
        route (format "%s%s" url send-route)
        compressed-batch (fressian/write messages)]
    (client/post route {:body (ByteBuffer/wrap (.array compressed-batch))}))
  {})

(defmethod extensions/internal-ack-message HttpKit
  [messenger event message-id acker-id completion-id ack-val]
  (let [replica @(:onyx.core/replica event)
        url (:url (get-in replica [:peer-site acker-id]))
        route (format "%s%s" url acker-route)
        contents (fressian/write {:id message-id :completion-id completion-id :ack-val ack-val})]
    (client/post route {:body (ByteBuffer/wrap (.array contents))})))

(defmethod extensions/internal-complete-message HttpKit
  [messenger id peer-id replica]
  (let [snapshot @replica
        url (:url (get-in replica [:peer-site peer-id]))
        route (format "%s%s" url completion-route)
        contents (fressian/write {:id id})]
    (client/post route {:body (ByteBuffer/wrap (.array contents))})))

