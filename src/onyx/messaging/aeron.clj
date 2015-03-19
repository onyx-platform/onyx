(ns ^:no-doc onyx.messaging.aeron
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close!]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [fatal] :as timbre]
              [onyx.messaging.protocol-aeron :as protocol]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.compression.nippy :refer [decompress compress]]
              [onyx.extensions :as extensions])
    (:import [uk.co.real_logic.aeron Aeron FragmentAssemblyAdapter]
             [uk.co.real_logic.aeron Aeron$Context]
             [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
             [uk.co.real_logic.agrona CloseHelper]
             [uk.co.real_logic.aeron.driver MediaDriver]
             [uk.co.real_logic.aeron.common.concurrent.logbuffer DataHandler]
             [uk.co.real_logic.aeron.common BackoffIdleStrategy]
             [java.util.function Consumer]
             [java.util.concurrent TimeUnit]))

(defn handle-sent-message [inbound-ch ^UnsafeBuffer buffer offset length header]
  (let [messages (protocol/read-messages-buf buffer offset length)]
    (doseq [message messages]
      (>!! inbound-ch message))))

(defn handle-acker-message [daemon buffer offset length header]
  (let [thawed (protocol/read-acker-message buffer offset)]
    (acker/ack-message daemon
                       (:id thawed)
                       (:completion-id thawed)
                       (:ack-val thawed))))

(defn handle-completion-message [release-ch buffer offset length header]
  (let [completion-id (protocol/read-completion buffer offset)]
    (>!! release-ch completion-id)))

(defn data-handler [f]
  (FragmentAssemblyAdapter. 
    (proxy [DataHandler] []
      (onData [buffer offset length header]
        (f buffer offset length header)))))

(defn consumer [limit]
  (proxy [Consumer] []
    (accept [subscription]
      ;; TODO, evaluate different idle strategies.
      (let [strategy :high-latency-restart
            idle-strategy (case strategy 
                            :high-latency-restart (BackoffIdleStrategy.
                                                    100
                                                    10
                                                    (.toNanos TimeUnit/MICROSECONDS 10000)
                                                    (.toNanos TimeUnit/MICROSECONDS 100000))
                            :low-latency-restart (BackoffIdleStrategy.
                                                   100 
                                                   10
                                                   (.toNanos TimeUnit/MICROSECONDS 1)
                                                   (.toNanos TimeUnit/MICROSECONDS 100)))]
        (while (not (Thread/interrupted))
          (let [fragments-read (.poll ^uk.co.real_logic.aeron.Subscription subscription limit)]
            (.idle idle-strategy fragments-read)))))))

(def no-op-error-handler
  (proxy [Consumer] []
    (accept [_] (prn "Conductor is down."))))

(def media-driver
  (MediaDriver/launch))

(defrecord AeronConnection [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron")

    (let [inbound-ch (:inbound-ch (:messenger-buffer component))
          daemon (:acking-daemon component)
          release-ch (chan (clojure.core.async/dropping-buffer 100000))

          ;; TODO, correct port allocation
          port (+ 40000 (rand-int 10000))
          channel (str "udp://localhost:" port)

          rand-stream-id (fn [] (rand-int 1000000))
          send-stream-id (rand-stream-id)
          acker-stream-id (rand-stream-id)
          completion-stream-id (rand-stream-id)

          ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
          aeron (Aeron/connect ctx)

          send-handler (data-handler (partial handle-sent-message inbound-ch))
          acker-handler (data-handler (partial handle-acker-message daemon))
          completion-handler (data-handler (partial handle-completion-message release-ch))

          send-subscriber (.addSubscription aeron channel send-stream-id send-handler)
          acker-subscriber (.addSubscription aeron channel acker-stream-id acker-handler)
          completion-subscriber (.addSubscription aeron channel completion-stream-id completion-handler)
          accept-send-fut (future (try (.accept ^Consumer (consumer 10) send-subscriber) 
                                       (catch Exception e (fatal e))))
          accept-acker-fut (future (try (.accept ^Consumer (consumer 10) acker-subscriber) 
                                        (catch Exception e (fatal e))))
          accept-completion-fut (future (try (.accept ^Consumer (consumer 10) completion-subscriber) 
                                             (catch Exception e (fatal e))))]
      (assoc component 
             :channel channel 
             :send-stream-id send-stream-id
             :acker-stream-id acker-stream-id 
             :completion-stream-id completion-stream-id
             :accept-send-fut accept-send-fut
             :accept-acker-fut accept-acker-fut
             :accept-completion-fut accept-completion-fut
             :send-subscriber send-subscriber
             :acker-subscriber acker-subscriber
             :completion-subscriber completion-subscriber
             :release-ch release-ch 
             :context ctx 
             :aeron aeron)))

  (stop [{:keys [aeron send-subscriber acker-subscriber 
                 completion-subscriber accept-send-fut accept-acker-fut
                 accept-completion-fut release-ch] :as component}]
    (taoensso.timbre/info "Stopping Aeron")

    (try 
      (future-cancel accept-send-fut)
      (future-cancel accept-acker-fut)
      (future-cancel accept-completion-fut)
      (when send-subscriber (.close send-subscriber))
      (when acker-subscriber (.close acker-subscriber))
      (when completion-subscriber (.close completion-subscriber))
      (when aeron (.close aeron))
      (close! (:release-ch component))
      ;; FIXME, need to startup and shutdown aeron resources properly
      ;; however, this can't be done per connection
      ;(CloseHelper/quietClose (:driver component))

      (catch Exception e
        (fatal e)))

    (assoc component :driver nil :channel nil :release-ch nil :aeron nil 
           :accept-send-fut nil :accept-acker-fut nil :accept-completion-fut nil 
           :send-subscriber nil :acker-subscriber nil :completion-subscriber nil)))

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
        aeron (Aeron/connect ctx)
        pub (.addPublication aeron channel stream-id)]
    {:ctx ctx :conn aeron :pub pub}))

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
  [messenger event peer-link batch]
  (let [[len unsafe-buffer] (protocol/build-messages-msg-buf batch)
        pub ^uk.co.real_logic.aeron.Publication (:pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 len))]
    (while (not (offer-f)))))

(defmethod extensions/internal-ack-message AeronConnection
  [messenger event peer-link message-id completion-id ack-val]
  (let [unsafe-buffer (protocol/build-acker-message message-id completion-id ack-val)
        pub ^uk.co.real_logic.aeron.Publication (:pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/ack-msg-length))]
    (while (not (offer-f)))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [unsafe-buffer (protocol/build-completion-msg-buf id)
        pub ^uk.co.real_logic.aeron.Publication (:pub peer-link) 
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/completion-msg-length))]
    (while (not (offer-f)))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  (.close (:pub peer-link))
  (.close (:conn peer-link)) {})
