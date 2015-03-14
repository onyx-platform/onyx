(ns ^:no-doc onyx.messaging.http-kit
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close!]]
              [com.stuartsierra.component :as component]
              [org.httpkit.server :as server]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions]
              [gniazdo.core :as ws])
    (:import [java.nio ByteBuffer]))

(def send-route "/send")

(def acker-route "/ack")

(def completion-route "/completion")

(defn app [daemon inbound-ch release-ch request]
  (server/with-channel request channel
    (server/on-receive
     channel
     (fn [data]
       (let [thawed (decompress data)
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
               (>!! release-ch (:id thawed))))))))

(defrecord HttpKitWebSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HTTP Kit WebSockets")

    (let [ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 100000))
          daemon (:acking-daemon component)
          ip "0.0.0.0"
          server (server/run-server (partial app daemon ch release-ch) {:ip ip :port 0 :thread 1 :queue-size 100000})]
      (assoc component :server server :ip ip :port (:local-port (meta server)) :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping HTTP Kit WebSockets")

    (close! (:release-ch component))
    ((:server component))
    (assoc component :release-ch nil)))

(defn http-kit-websockets [opts]
  (map->HttpKitWebSockets {:opts opts}))

(defmethod extensions/send-peer-site HttpKitWebSockets
  [messenger]
  {:url (format "ws://%s:%s%s" (:ip messenger) (:port messenger) send-route)})

(defmethod extensions/acker-peer-site HttpKitWebSockets
  [messenger]
  {:url (format "ws://%s:%s%s" (:ip messenger) (:port messenger) acker-route)})

(defmethod extensions/completion-peer-site HttpKitWebSockets
  [messenger]
  {:url (format "ws://%s:%s%s" (:ip messenger) (:port messenger) completion-route)})

(defmethod extensions/connect-to-peer HttpKitWebSockets
  [messenger event peer-site]
  (ws/connect (:url peer-site)))

(defmethod extensions/receive-messages HttpKitWebSockets
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 1000)
        ch (:inbound-ch (:onyx.core/messenger-buffer event))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i (:onyx/batch-size task-map))
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defmethod extensions/send-messages HttpKitWebSockets
  [messenger event peer-link compressed-batch]
  (ws/send-msg peer-link compressed-batch))

(defmethod extensions/internal-ack-message HttpKitWebSockets
  [messenger event peer-link message-id completion-id ack-val]
  (let [contents (compress {:id message-id :completion-id completion-id :ack-val ack-val})]
    (ws/send-msg peer-link contents)))

(defmethod extensions/internal-complete-message HttpKitWebSockets
  [messenger event id peer-link]
  (let [contents (compress {:id id})]
    (ws/send-msg peer-link contents)))

(defmethod extensions/close-peer-connection HttpKitWebSockets
  [messenger event peer-link]
  (ws/close peer-link))

