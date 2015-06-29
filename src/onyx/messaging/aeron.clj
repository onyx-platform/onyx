(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal] :as timbre]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.acking-daemon :as acker]
            [onyx.messaging.common :as common]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.static.default-vals :refer [defaults]])
  (:import [uk.co.real_logic.aeron Aeron FragmentAssemblyAdapter]
           [uk.co.real_logic.aeron Aeron$Context]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler]
           [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
           [uk.co.real_logic.agrona CloseHelper]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(defrecord AeronPeerGroup [opts]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (if-not (nil? (:onyx.messaging.aeron/embedded-driver? opts))
                             (:onyx.messaging.aeron/embedded-driver? opts)
                             (:onyx.messaging.aeron/embedded-driver? defaults))]
      (if embedded-driver?
        (let [ctx (doto (MediaDriver$Context.) 
                    (.threadingMode ThreadingMode/DEDICATED)
                    (.dirsDeleteOnExit true))
              media-driver (MediaDriver/launch ctx)]
          (assoc component :media-driver media-driver)) 
        component)))

  (stop [{:keys [media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (when media-driver (.close ^MediaDriver media-driver))
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
    (when-not port
      (throw (ex-info "Couldn't assign port - ran out of available ports.
                      Available ports can be configured in the peer-config.
                      e.g. {:onyx.messaging/peer-ports [40000, 40002],
                            :onyx.messaging/peer-port-range [40200 40260]}"
                      peer-site))) 
    {:aeron/port port}))

(defmethod extensions/get-peer-site :aeron
  [replica peer]
  (get-in replica [:peer-sites peer :aeron/external-addr]))

(defn handle-sent-message [inbound-ch decompress-f ^UnsafeBuffer buffer offset length header]
  (let [messages (protocol/read-messages-buf decompress-f buffer offset length)]
    (doseq [message messages]
      (>!! inbound-ch message))))

(defn handle-aux-message [daemon release-ch retry-ch buffer offset length header]
  (let [msg-type (protocol/read-message-type buffer offset)
        offset-rest (long (inc offset))] 
    (cond (= msg-type protocol/ack-msg-id)
          (let [ack (protocol/read-acker-message buffer offset-rest)]
            (acker/ack-message daemon (:id ack) (:completion-id ack) (:ack-val ack)))

          (= msg-type protocol/completion-msg-id)
          (let [completion-id (protocol/read-completion buffer offset-rest)]
            (>!! release-ch completion-id))

          (= msg-type protocol/retry-msg-id)
          (let [retry-id (protocol/read-retry buffer offset-rest)]
            (>!! retry-ch retry-id)))))

(defn data-handler [f]
  (FragmentAssemblyAdapter. 
    (proxy [FragmentHandler] []
      (onFragment [buffer offset length header]
        (f buffer offset length header)))))

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 1 
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 10
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 100)
                                                (.toNanos TimeUnit/MICROSECONDS 10000))))

(defn consumer [handler ^IdleStrategy idle-strategy limit]
  (proxy [Consumer] []
    (accept [subscription]
      (while (not (Thread/interrupted))
        (let [fragments-read (.poll ^uk.co.real_logic.aeron.Subscription subscription ^FragmentHandler handler ^int limit)]
          (.idle idle-strategy fragments-read))))))

(def no-op-error-handler
  (proxy [ErrorHandler] []
    (onError [x] (taoensso.timbre/warn x))))

(defrecord AeronConnection [peer-group bind-addr external-addr 
                            inbound-ch send-idle-strategy receive-idle-strategy ports resources 
                            release-ch retry-ch decompress-f compress-f]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron")
    (let [config (:config peer-group)
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (sliding-buffer (:onyx.messaging/release-ch-buffer-size defaults)))
          retry-ch (chan (sliding-buffer (:onyx.messaging/retry-ch-buffer-size defaults)))
          bind-addr (common/bind-addr config)
          external-addr (common/external-addr config)
          ports (common/allowable-ports config)
          idle-strategy-config (or (:onyx.messaging.aeron/idle-strategy config) 
                                   (:onyx.messaging.aeron/idle-strategy defaults))
          send-idle-strategy (backoff-strategy idle-strategy-config)
          receive-idle-strategy (backoff-strategy idle-strategy-config)]
      (assoc component 
             :bind-addr bind-addr 
             :external-addr external-addr
             :inbound-ch inbound-ch
             :send-idle-strategy send-idle-strategy
             :receive-idle-strategy receive-idle-strategy
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
                      aux-subscriber 
                      accept-send-fut 
                      accept-aux-fut]} rs] 
          (future-cancel accept-send-fut)
          (future-cancel accept-aux-fut)
         (when send-subscriber (.close ^uk.co.real_logic.aeron.Subscription send-subscriber))
         (when aux-subscriber (.close ^uk.co.real_logic.aeron.Subscription aux-subscriber))
         (when conn (.close ^uk.co.real_logic.aeron.Aeron conn)))
        (reset! resources nil))
      (close! (:release-ch component))
      (close! (:retry-ch component))
      (catch Throwable e (fatal e)))

    (assoc component
           :bind-addr nil :external-addr nil :inbound-ch nil :send-idle-strategy nil 
           :receive-idle-strategy nil :ports nil :resources nil
           :release-ch nil :retry-ch nil :decompress-f nil :compress-f nil)))

(defn aeron [peer-group]
  (map->AeronConnection {:peer-group peer-group}))

(defmethod extensions/peer-site AeronConnection
  [messenger]
  {:aeron/ports (:ports messenger)
   :aeron/external-addr (:external-addr messenger)})

(def send-stream-id 1)
(def aux-stream-id 2)

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defrecord AeronResources [conn accept-send-fut accept-aux-fut send-subscriber aux-subscriber])

(defmethod extensions/open-peer-site AeronConnection
  [messenger assigned]
  (let [inbound-ch (:inbound-ch (:messenger-buffer messenger))
        {:keys [release-ch retry-ch acking-daemon bind-addr]} messenger
        ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)
        channel (aeron-channel bind-addr (:aeron/port assigned))
        decompress-f (:decompress-f messenger)
        send-handler (data-handler (fn [buffer offset length header] 
                                     (handle-sent-message inbound-ch decompress-f buffer offset length header)))
        aux-handler (data-handler (fn [buffer offset length header] 
                                    (handle-aux-message acking-daemon release-ch retry-ch buffer offset length header)))

        send-subscriber (.addSubscription aeron channel send-stream-id)
        aux-subscriber (.addSubscription aeron channel aux-stream-id)

        receive-idle-strategy (:receive-idle-strategy messenger)

        ;; pass in handler to consumer constructor
        ;;;;; FIXME: 10 is not the right fragment limit to use here
        ;;;;; should at least be configurable
        accept-send-fut (future (try (.accept ^Consumer (consumer send-handler receive-idle-strategy 10) send-subscriber) 
                                     (catch Throwable e (fatal e))))
        accept-aux-fut (future (try (.accept ^Consumer (consumer aux-handler receive-idle-strategy 10) aux-subscriber) 
                                      (catch Throwable e (fatal e))))]
    (reset! (:resources messenger)
            (->AeronResources aeron accept-send-fut accept-aux-fut send-subscriber aux-subscriber))))

(defrecord AeronPeerConnection [conn send-pub-f-create send-pub aux-pub-f-create aux-pub])

(defmethod extensions/connect-to-peer AeronConnection
  [messenger event {:keys [aeron/external-addr aeron/port]}]
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)
        channel (aeron-channel external-addr port)
        f-send-pub #(.addPublication aeron channel send-stream-id)
        f-aux-pub #(.addPublication aeron channel aux-stream-id)]
    (->AeronPeerConnection aeron f-send-pub (atom nil) f-aux-pub (atom nil))))

(defmethod extensions/receive-messages AeronConnection
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ch (:inbound-ch messenger)
        batch-size (:onyx/batch-size task-map)
        ms (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i batch-size)
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defn get-peer-link-pub [peer-link link-key create-key]
  (if-let [pub @(get peer-link link-key)]
    pub
    (reset! (get peer-link link-key) ((get peer-link create-key)))))

(defmethod extensions/send-messages AeronConnection
  [messenger event peer-link batch]
  (let [[len unsafe-buffer] (protocol/build-messages-msg-buf (:compress-f messenger) batch)
        pub ^uk.co.real_logic.aeron.Publication (get-peer-link-pub peer-link :send-pub :send-pub-f-create)
        offer-f (fn [] (.offer pub unsafe-buffer 0 len))
        idle-strategy (:send-idle-strategy messenger)]
    (while (not (offer-f))
      (.idle ^IdleStrategy idle-strategy 0))))

(defmethod extensions/internal-ack-messages AeronConnection
  [messenger event peer-link acks]
  ; TODO: Might want to batch in a single buffer as in netty
  (let [pub ^uk.co.real_logic.aeron.Publication (get-peer-link-pub peer-link :aux-pub :aux-pub-f-create)
        idle-strategy (:send-idle-strategy messenger)] 
    (doseq [{:keys [id completion-id ack-val]} acks] 
      (let [unsafe-buffer (protocol/build-acker-message id completion-id ack-val)
            offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/ack-msg-length))]
        (while (not (offer-f))
          (.idle ^IdleStrategy idle-strategy 0))))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [idle-strategy (:send-idle-strategy messenger)
        unsafe-buffer (protocol/build-completion-msg-buf id)
        pub ^uk.co.real_logic.aeron.Publication (get-peer-link-pub peer-link :aux-pub :aux-pub-f-create)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/completion-msg-length))]
    (while (not (offer-f))
      (.idle ^IdleStrategy idle-strategy 0))))

(defmethod extensions/internal-retry-message AeronConnection
  [messenger event id peer-link]
  (let [idle-strategy (:send-idle-strategy messenger)
        unsafe-buffer (protocol/build-retry-msg-buf id)
        pub ^uk.co.real_logic.aeron.Publication (get-peer-link-pub peer-link :aux-pub :aux-pub-f-create)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/retry-msg-length))]
    (while (not (offer-f))
      (.idle ^IdleStrategy idle-strategy 0))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  (when-let [pub (get peer-link :send-pub)] 
    (when @pub (.close ^uk.co.real_logic.aeron.Publication @pub))
    (reset! pub nil))
  (when-let [pub (get peer-link :aux-pub)]
    (when @pub (.close ^uk.co.real_logic.aeron.Publication @pub))
    (reset! pub nil))
  (.close ^uk.co.real_logic.aeron.Aeron (:conn peer-link)) 
  {})
