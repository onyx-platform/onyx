(ns ^:no-doc onyx.messaging.aeron
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.compression.nippy :refer [compress decompress]]
              [onyx.extensions :as extensions])
    (:import [uk.co.real_logic.aeron.driver MediaDriver]
             [uk.co.real_logic.aeron Aeron]
             [uk.co.real_logic.aeron Aeron$Context]
             [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
             [uk.co.real_logic.agrona CloseHelper]
             [java.nio ByteBuffer]))

(defn handle-sent-message [inbound-ch buffer offset length header]
  (let [thawed (decompress buffer)]
    (doseq [message thawed]
      (>!! inbound-ch message))))

(defn handle-acker-message [daemon buffer offset length header]
  (let [thawed (decompress buffer)]
    (acker/ack-message daemon
                       (:id thawed)
                       (:completion-id thawed)
                       (:ack-val thawed))))

(defn handle-completion-message [release-ch buffer offset length header]
  (let [thawed (decompress buffer)]
    (>!! release-ch (:id thawed))))

(defrecord HttpKitWebSockets [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron")

    (let [inbound-ch (:inbound-ch (:messenger-buffer component))
          daemon (:acking-daemon component)
          release-ch (chan (clojure.core.async/dropping-buffer 100000))

          rand-stream-id (fn [] (rand-int 100000000))
          send-stream-id (rand-stream-id)
          acker-stream-id (rand-stream-id)
          completion-stream-id (rand-stream-id)

          driver (MediaDriver/launch)
          ctx (Aeron$Context.)
          aeron (Aeron/connect ctx)

          send-handler (AeronSubscriberShim/handleMessage (partial handle-sent-message inbound-ch))
          acker-handler (AeronSubscriberShim/handleMessage (partial handle-acker-message daemon))
          completion-handler (AeronSubscriberShim/handleMessage (partial handle-completion-message release-ch))

          send-subscriber (.addSubscription aeron "udp://localhost:40123" send-stream-id send-handler)
          acker-subscriber (.addSubscription aeron "udp://localhost:40123" acker-stream-id acker-handler)
          completion-subscriber (.addSubscription aeron "udp://localhost:40123" completion-stream-id completion-handler)]

      (.accept (SamplesUtil/subscriberLoop 10 running) subscription)
      
      (assoc component :driver driver))

    (let [ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (clojure.core.async/dropping-buffer 100000))
          daemon (:acking-daemon component)
          ip "0.0.0.0"
          server (server/run-server (partial app daemon ch release-ch) {:ip ip :port 0 :thread 1 :queue-size 100000})]
      (assoc component :server server :ip ip :port (:local-port (meta server)) :release-ch release-ch)))

  (stop [component]
    (taoensso.timbre/info "Stopping Aeron")

    (close! (:release-ch component))
    ((:server component))
    (assoc component :release-ch nil)))

(defn aeron [opts]
  (map->Aeron {:opts opts}))

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
  [messenger event peer-link]
  (let [messages (:onyx.core/compressed event)
        compressed-batch (compress messages)]
    (ws/send-msg peer-link compressed-batch)))

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

