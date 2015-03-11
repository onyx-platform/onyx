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
             [uk.co.real_logic.aeron.common.concurrent.logbuffer DataHandler]
             [uk.co.real_logic.aeron.common BackoffIdleStrategy]
             [java.util.function Consumer]
             [java.util.concurrent TimeUnit]
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

(defn data-handler [f]
  (proxy [DataHandler] []
    (onData [buffer offset length header]
      (f buffer offset length header))))

(defn consumer [limit]
  (proxy [Consumer] []
    (accept [subscription]
      (let [idle-strategy (BackoffIdleStrategy.
                           100 10
                           (.toNanos TimeUnit/MICROSECONDS 1)
                           (.toNanos TimeUnit/MICROSECONDS 100))]
        (while true
          (let [fragments-read (.poll subscription limit)]
            (.idle idle-strategy fragments-read)))))))

(defrecord AeronConnection [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron")

    (let [inbound-ch (:inbound-ch (:messenger-buffer component))
          daemon (:acking-daemon component)
          release-ch (chan (clojure.core.async/dropping-buffer 100000))

          port (+ 40000 (rand-int 250))
          channel (str "udp://localhost:" port)

          rand-stream-id (fn [] (rand-int 1000000))
          send-stream-id (rand-stream-id)
          acker-stream-id (rand-stream-id)
          completion-stream-id (rand-stream-id)

          _ (prn "Driver ...")
          driver (MediaDriver/launch)
          _ (prn "Ctd ...")
          ctx (Aeron$Context.)
          _ (prn "Aeron ...")
          aeron (Aeron/connect ctx)
          _ (prn "Done")

          send-handler (data-handler (partial handle-sent-message inbound-ch))
          acker-handler (data-handler (partial handle-acker-message daemon))
          completion-handler (data-handler (partial handle-completion-message release-ch))

          _ (prn "Add")
          send-subscriber (.addSubscription aeron channel send-stream-id send-handler)
          _ (prn "Next")
          acker-subscriber (.addSubscription aeron channel acker-stream-id acker-handler)
          completion-subscriber (.addSubscription aeron channel completion-stream-id completion-handler)]

      (prn "La")
      (future (.accept (consumer 10) send-subscriber))
      (prn "Ba")
      (future (.accept (consumer 10) acker-subscriber))
      (prn "Ma")
      (future (.accept (consumer 10) completion-subscriber))
      (prn "Eee")
      
      (assoc component :driver driver :channel channel :send-stream-id send-stream-id
             :acker-stream-id acker-stream-id :completion-stream-id completion-stream-id)))

  (stop [component]
    (taoensso.timbre/info "Stopping Aeron")

    (CloseHelper/quietClose (:driver component))
    (assoc component :driver nil :channel nil)))

(defn aeron [opts]
  (map->AeronConnection {:opts opts}))

(defmethod extensions/send-peer-site AeronConnection
  [messenger]
  {:channel (:channel messenger)
   :send-stream-id (:send-stream-id messenger)})

(defmethod extensions/acker-peer-site AeronConnection
  [messenger]
  {:channel (:channel messenger)
   :acker-stream-id (:acker-stream-id messenger)})

(defmethod extensions/completion-peer-site AeronConnection
  [messenger]
  {:channel (:channel messenger)
   :completion-stream-id (:completion-stream-id messenger)})

(defmethod extensions/connect-to-peer AeronConnection
  [messenger event {:keys [channel send-stream-id]}]
  (let [ctx (Aeron$Context.)
        aeron (Aeron/connect ctx)]
    (.addPublication aeron channel send-stream-id))  )

(defmethod extensions/receive-messages AeronConnection
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

(defmethod extensions/send-messages AeronConnection
  [messenger event peer-link]
  (let [messages (:onyx.core/compressed event)
        compressed-batch (compress messages)
        len (count (.getBytes compressed-batch))
        unsafe-buffer (UnsafeBuffer. (ByteBuffer/allocateDirect len))]
    (.offer peer-link unsafe-buffer 0 len)))

(defmethod extensions/internal-ack-message AeronConnection
  [messenger event peer-link message-id completion-id ack-val]
  (let [contents (compress {:id message-id :completion-id completion-id :ack-val ack-val})
        len (count (.getBytes contents))
        unsafe-buffer (UnsafeBuffer. (ByteBuffer/allocateDirect len))]
    (.offer peer-link unsafe-buffer 0 len)))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [contents (compress {:id id})
        len (count (.getBytes contents))
        unsafe-buffer (UnsafeBuffer. (ByteBuffer/allocateDirect len))]
    (.offer peer-link unsafe-buffer 0 len)))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  {})

