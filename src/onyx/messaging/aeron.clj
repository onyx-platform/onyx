(ns ^:no-doc onyx.messaging.aeron
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [fatal] :as timbre]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.compression.nippy :refer [decompress compress]]
              [onyx.extensions :as extensions])
    (:import [uk.co.real_logic.aeron Aeron]
             [uk.co.real_logic.aeron Aeron$Context]
             [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
             [uk.co.real_logic.agrona CloseHelper]
             [uk.co.real_logic.aeron.driver MediaDriver]
             [uk.co.real_logic.aeron.common.concurrent.logbuffer DataHandler]
             [uk.co.real_logic.aeron.common BackoffIdleStrategy]
             [java.util.function Consumer]
             [java.util.concurrent TimeUnit]))

(defn handle-sent-message [inbound-ch buffer offset length header]
  (let [dst (byte-array length)]
    (.getBytes buffer offset dst)
    (let [thawed (decompress dst)]
      (doseq [message dst]
        (>!! inbound-ch message)))))

(defn handle-acker-message [daemon buffer offset length header]
  (let [dst (byte-array length)]
    (.getBytes buffer offset dst)
    (let [thawed (decompress dst)]
      (acker/ack-message daemon
                         (:id thawed)
                         (:completion-id thawed)
                         (:ack-val thawed)))))

(defn handle-completion-message [release-ch buffer offset length header]
  (let [dst (byte-array length)]
    (.getBytes buffer offset dst)
    (let [thawed (decompress dst)]
      (>!! release-ch (:id thawed)))))

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

(def no-op-error-handler
  (proxy [Consumer] []
    (accept [_] (prn "Conductor is down."))))

(defrecord AeronConnection [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron")

    (let [inbound-ch (:inbound-ch (:messenger-buffer component))
          daemon (:acking-daemon component)
          release-ch (chan (clojure.core.async/dropping-buffer 100000))

          port (+ 40000 (rand-int 10000))
          channel (str "udp://localhost:" port)

          rand-stream-id (fn [] (rand-int 1000000))
          send-stream-id (rand-stream-id)
          acker-stream-id (rand-stream-id)
          completion-stream-id (rand-stream-id)

          driver (MediaDriver/launch)
          ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
          aeron (Aeron/connect ctx)

          send-handler (data-handler (partial handle-sent-message inbound-ch))
          acker-handler (data-handler (partial handle-acker-message daemon))
          completion-handler (data-handler (partial handle-completion-message release-ch))

          send-subscriber (.addSubscription aeron channel send-stream-id send-handler)
          acker-subscriber (.addSubscription aeron channel acker-stream-id acker-handler)
          completion-subscriber (.addSubscription aeron channel completion-stream-id completion-handler)]

      (future (try (.accept (consumer 10) send-subscriber) (catch Exception e (fatal e))))
      (future (try (.accept (consumer 10) acker-subscriber) (catch Exception e (fatal e))))
      (future (try (.accept (consumer 10) completion-subscriber) (catch Exception e (fatal e))))
      
      (assoc component :driver driver :channel channel :send-stream-id send-stream-id
             :acker-stream-id acker-stream-id :completion-stream-id completion-stream-id
             :release-ch release-ch :context ctx :aeron aeron)))

  (stop [component]
    (taoensso.timbre/info "Stopping Aeron")

    (.close (:aeron component))
    (CloseHelper/quietClose (:driver component))

    (close! (:release-ch component))

    (assoc component :driver nil :channel nil :release-ch nil)))

(defn aeron [opts]
  (map->AeronConnection {:opts opts}))

(defmethod extensions/send-peer-site AeronConnection
  [messenger]
  {:channel (:channel messenger)
   :stream-id (:send-stream-id messenger)})

(defmethod extensions/acker-peer-site AeronConnection
  [messenger]
  {:channel (:channel messenger)
   :stream-id (:acker-stream-id messenger)})

(defmethod extensions/completion-peer-site AeronConnection
  [messenger]
  {:channel (:channel messenger)
   :stream-id (:completion-stream-id messenger)})

(defmethod extensions/connect-to-peer AeronConnection
  [messenger event {:keys [channel stream-id]}]
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)]
    {:ctx ctx
     :conn aeron
     :pub (.addPublication aeron channel stream-id)}))

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
  [messenger event peer-link compressed]
  (let [len (count compressed)
        unsafe-buffer (UnsafeBuffer. compressed)
        offer-f (fn [] (.offer (:pub peer-link) unsafe-buffer 0 len))]
    (while (not (offer-f)))))

(defmethod extensions/internal-ack-message AeronConnection
  [messenger event peer-link message-id completion-id ack-val]
  (let [compressed (compress {:id message-id :completion-id completion-id :ack-val ack-val})
        len (count compressed)
        unsafe-buffer (UnsafeBuffer. compressed)
        offer-f (fn [] (.offer (:pub peer-link) unsafe-buffer 0 len))]
    (while (not (offer-f)))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [compressed (compress {:id id})
        len (count compressed)
        unsafe-buffer (UnsafeBuffer. compressed)
        offer-f (fn [] (.offer (:pub peer-link) unsafe-buffer 0 len))]
    (while (not (offer-f)))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  (.close (:pub peer-link))
  (.close (:conn peer-link)) {})

