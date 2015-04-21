(ns ^:no-doc onyx.messaging.aeron
    (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! dropping-buffer]]
              [com.stuartsierra.component :as component]
              [taoensso.timbre :refer [fatal] :as timbre]
              [onyx.messaging.protocol-aeron :as protocol]
              [onyx.messaging.acking-daemon :as acker]
              [onyx.messaging.common :refer [bind-addr external-addr allowable-ports]]
              [onyx.extensions :as extensions]
              [onyx.compression.nippy :refer [compress decompress]])
    (:import [uk.co.real_logic.aeron Aeron FragmentAssemblyAdapter]
             [uk.co.real_logic.aeron Aeron$Context]
             [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
             [uk.co.real_logic.aeron.common.concurrent.logbuffer DataHandler]
             [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
             [uk.co.real_logic.agrona CloseHelper]
             [uk.co.real_logic.agrona.concurrent IdleStrategy BackoffIdleStrategy]
             [java.util.function Consumer]
             [java.util.concurrent TimeUnit]))

(defrecord AeronPeerGroup [opts]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (if (and (contains? opts :onyx.messaging.aeron/embedded-driver)
             (false? (opts :onyx.messaging.aeron/embedded-driver)))
      component
      (let [ctx (doto (MediaDriver$Context.) 
                  (.threadingMode ThreadingMode/SHARED)
                  (.dirsDeleteOnExit true))
            media-driver (MediaDriver/launch ctx)]
        (assoc component :media-driver media-driver))))

  (stop [{:keys [media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (when media-driver (.close media-driver))
    (assoc component :media-driver nil)))

(defn aeron-peer-group [opts]
  (map->AeronPeerGroup {:opts opts}))

(defmethod extensions/assign-site-resources :aeron
  [config peer-site peer-sites]
  (let [used-ports (->> (vals peer-sites) 
                        (filter 
                          (fn [s]
                            (= (:aeron/external-addr peer-site) 
                               (:aeron/external-addr s))))
                        (map :aeron/port)
                        set)
        port (first (sort (remove used-ports (:aeron/ports peer-site))))]
    (assert port "Couldn't assign port - ran out of available ports.")
    {:aeron/port port}))

(defn handle-sent-message [inbound-ch decompress-f ^UnsafeBuffer buffer offset length header]
  (let [messages (protocol/read-messages-buf decompress-f buffer offset length)]
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

(defn handle-retry-message [retry-ch buffer offset length header]
  (let [retry-id (protocol/read-retry buffer offset)]
    (>!! retry-ch retry-id)))

(defn data-handler [f]
  (FragmentAssemblyAdapter. 
    (proxy [DataHandler] []
      (onData [buffer offset length header]
        (f buffer offset length header)))))

(defn backoff-strategy [strategy]
  (case strategy
    :low-restart-latency (BackoffIdleStrategy. 100 
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 10
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10000)
                                                (.toNanos TimeUnit/MICROSECONDS 100000))))

(defn consumer [idle-strategy limit]
  (proxy [Consumer] []
    (accept [subscription]
      ;; TODO, evaluate different idle strategies.
      (let [strategy ^IdleStrategy (backoff-strategy idle-strategy)]
        (while (not (Thread/interrupted))
          (let [fragments-read (.poll ^uk.co.real_logic.aeron.Subscription subscription limit)]
            (.idle strategy fragments-read)))))))

(def no-op-error-handler
  (proxy [Consumer] []
    (accept [x] (taoensso.timbre/warn x))))

(defrecord AeronConnection [peer-group]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron")
    (let [config (:config peer-group)
          release-ch (chan (dropping-buffer 10000))
          retry-ch (chan (dropping-buffer 10000))
          bind-addr (bind-addr config)
          external-addr (external-addr config)
          ports (allowable-ports config)
          backpressure-strategy (or (:onyx.messaging.aeron/backpressure-strategy config) 
                                    :high-restart-latency)]
      (assoc component 
        :bind-addr bind-addr 
        :external-addr external-addr
        :backpressure-strategy backpressure-strategy
        :ports ports
        :resources (atom nil) 
        :release-ch release-ch
        :retry-ch retry-ch
        :decompress-f (or (:onyx.messaging/decompress-fn (:config peer-group)) decompress)
        :compress-f (or (:onyx.messaging/compress-fn (:config peer-group)) compress))))

  (stop [{:keys [aeron resources release-ch] :as component}]
    (taoensso.timbre/info "Stopping Aeron")
    (try 
      (when-let [rs @resources]
        (let [{:keys [conn
                      send-subscriber 
                      acker-subscriber 
                      completion-subscriber
                      retry-subscriber
                      accept-send-fut 
                      accept-acker-fut
                      accept-completion-fut
                      accept-retry-fut]} rs] 
          (future-cancel accept-send-fut)
          (future-cancel accept-acker-fut)
          (future-cancel accept-completion-fut)
          (future-cancel accept-retry-fut)
         (when send-subscriber (.close send-subscriber))
         (when acker-subscriber (.close acker-subscriber))
         (when completion-subscriber (.close completion-subscriber))
         (when retry-subscriber (.close retry-subscriber))
         (when conn (.close conn)))
        (reset! resources nil))
      (close! (:release-ch component))
      (close! (:retry-ch component))
      (catch Throwable e (fatal e)))

    (assoc component
      :bind-addr nil :external-addr nil
      :site-resources nil :release-ch nil :retry-ch nil)))

(defn aeron [peer-group]
  (map->AeronConnection {:peer-group peer-group}))

(defmethod extensions/peer-site AeronConnection
  [messenger]
  {:aeron/ports (:ports messenger)
   :aeron/external-addr (:external-addr messenger)})

(def send-stream-id 1)
(def acker-stream-id 2)
(def completion-stream-id 3)
(def retry-stream-id 4)

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defmethod extensions/open-peer-site AeronConnection
  [messenger assigned]
  (let [inbound-ch (:inbound-ch (:messenger-buffer messenger))
        {:keys [release-ch retry-ch acking-daemon backpressure-strategy bind-addr]} messenger
        ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)
        channel (aeron-channel bind-addr (:aeron/port assigned))
        send-idle-strategy (backoff-strategy backpressure-strategy)

        send-handler (data-handler (partial handle-sent-message inbound-ch (:decompress-f messenger)))
        acker-handler (data-handler (partial handle-acker-message acking-daemon))
        completion-handler (data-handler (partial handle-completion-message release-ch))
        retry-handler (data-handler (partial handle-retry-message retry-ch))

        send-subscriber (.addSubscription aeron channel send-stream-id send-handler)
        acker-subscriber (.addSubscription aeron channel acker-stream-id acker-handler)
        completion-subscriber (.addSubscription aeron channel completion-stream-id completion-handler)
        retry-subscriber (.addSubscription aeron channel retry-stream-id retry-handler)

        accept-send-fut (future (try (.accept ^Consumer (consumer backpressure-strategy 10) send-subscriber) 
                                     (catch Throwable e (fatal e))))
        accept-acker-fut (future (try (.accept ^Consumer (consumer backpressure-strategy 10) acker-subscriber) 
                                      (catch Throwable e (fatal e))))
        accept-completion-fut (future (try (.accept ^Consumer (consumer backpressure-strategy 10) completion-subscriber) 
                                           (catch Throwable e (fatal e))))
        accept-retry-fut (future (try (.accept ^Consumer (consumer backpressure-strategy 10) retry-subscriber)
                                      (catch Throwable e (fatal e))))]
    (reset! (:resources messenger)
            {:conn aeron
             :send-idle-strategy send-idle-strategy
             :accept-send-fut accept-send-fut
             :accept-acker-fut accept-acker-fut
             :accept-completion-fut accept-completion-fut
             :accept-retry-fut accept-retry-fut
             :send-subscriber send-subscriber
             :acker-subscriber acker-subscriber
             :completion-subscriber completion-subscriber
             :retry-subscriber retry-subscriber})))

(defmethod extensions/connect-to-peer AeronConnection
  [messenger event {:keys [aeron/external-addr aeron/port]}]
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)
        channel (aeron-channel external-addr port)
        send-pub (.addPublication aeron channel send-stream-id)
        acker-pub (.addPublication aeron channel acker-stream-id)
        completion-pub (.addPublication aeron channel completion-stream-id)
        retry-pub (.addPublication aeron channel retry-stream-id)]
    {:conn aeron :send-pub send-pub :acker-pub acker-pub
     :completion-pub completion-pub :retry-pub retry-pub}))

(defmethod extensions/receive-messages AeronConnection
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ms (or (:onyx/batch-timeout task-map) 50)
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
  (let [[len unsafe-buffer] (protocol/build-messages-msg-buf (:compress-f messenger) batch)
        pub ^uk.co.real_logic.aeron.Publication (:send-pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 len))]
    (while (not (offer-f))
      (.idle ^IdleStrategy (:send-idle-strategy messenger) 0))))

(defmethod extensions/internal-ack-message AeronConnection
  [messenger event peer-link message-id completion-id ack-val]
  (let [unsafe-buffer (protocol/build-acker-message message-id completion-id ack-val)
        pub ^uk.co.real_logic.aeron.Publication (:acker-pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/ack-msg-length))]
    (while (not (offer-f))
      (.idle ^IdleStrategy (:send-idle-strategy messenger) 0))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [unsafe-buffer (protocol/build-completion-msg-buf id)
        pub ^uk.co.real_logic.aeron.Publication (:completion-pub peer-link) 
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/completion-msg-length))]
    (while (not (offer-f))
      (.idle ^IdleStrategy (:send-idle-strategy messenger) 0))))

(defmethod extensions/internal-retry-message AeronConnection
  [messenger event id peer-link]
  (let [unsafe-buffer (protocol/build-retry-msg-buf id)
        pub ^uk.co.real_logic.aeron.Publication (:retry-pub peer-link)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/retry-msg-length))]
    (while (not (offer-f))
      (.idle ^IdleStrategy (:send-idle-strategy messenger) 0))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  (.close (:send-pub peer-link))
  (.close (:acker-pub peer-link))
  (.close (:retry-pub peer-link))
  (.close (:completion-pub peer-link))
  (.close (:conn peer-link)) 
  {})
