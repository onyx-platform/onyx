(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal] :as timbre]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
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

(def send-stream-id 1)

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defrecord AeronConnection 
  [peer-group messaging-group acking-daemon acking-ch 
   send-idle-strategy compress-f inbound-ch release-ch retry-ch]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron")
    (let [config (:config peer-group)
          messaging-group (:messaging-group peer-group)
          publications (:publications messaging-group)
          inbound-ch (:inbound-ch (:messenger-buffer component))
          release-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/release-ch-buffer-size config)))
          retry-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/retry-ch-buffer-size config)))
          acking-ch (:acking-ch (:acking-daemon component))
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f (:messaging-group peer-group))]
      (assoc component 
             :messaging-group messaging-group
             :publications publications
             :send-idle-strategy send-idle-strategy
             :compress-f compress-f
             :acking-ch acking-ch
             :inbound-ch inbound-ch
             :release-ch release-ch
             :retry-ch retry-ch)))

  (stop [{:keys [aeron resources release-ch] :as component}]
    (taoensso.timbre/info "Stopping Aeron")
    (try 
      ;;; TODO; could handle the inbound-ch in a similar way - rather than using messenger-buffer
      (close! (:release-ch component))
      (close! (:retry-ch component))
      (catch Throwable e (fatal e)))

    (assoc component
           :send-idle-strategy nil 
           :compress-f nil :decompress-f nil 
           :inbound-ch nil :release-ch nil :retry-ch nil )))

(defmethod extensions/open-peer-site AeronConnection
  [messenger assigned]
  (swap! (:virtual-peers (:messaging-group (:peer-group messenger)))
         assoc 
         (:id messenger)
         messenger))

(def no-op-error-handler
  (reify ErrorHandler 
    (onError [this x] (taoensso.timbre/warn x))))

(defn fragment-data-handler [f]
  (FragmentAssemblyAdapter. 
    (reify FragmentHandler 
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 100
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 1000
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10)
                                                (.toNanos TimeUnit/MICROSECONDS 1000))))

(defn consumer [handler ^IdleStrategy idle-strategy limit]
  (reify Consumer 
    (accept [this subscription]
      (while (not (Thread/interrupted))
        (let [fragments-read (.poll ^uk.co.real_logic.aeron.Subscription subscription ^FragmentHandler handler ^int limit)]
          (.idle idle-strategy fragments-read))))))


(defn handle-message [decompress-f virtual-peers buffer offset length header]
  (let [msg-type (protocol/read-message-type buffer offset)
        offset-rest (inc ^long offset)] 
    (cond (= msg-type protocol/ack-msg-id)
          (let [message (protocol/read-acker-message buffer offset-rest)
                ack (:payload message)
                peer-id (:peer-id message)]
            (>!! (:acking-ch (get @virtual-peers peer-id)) ack))

          (= msg-type protocol/messages-msg-id)
          (let [message (protocol/read-messages-buf decompress-f buffer offset-rest length)
                segments (:payload message)
                peer-id (:peer-id message)
                inbound-ch (:inbound-ch (get @virtual-peers peer-id))]
            (doseq [segment segments]
              (>!! inbound-ch segment)))

          (= msg-type protocol/completion-msg-id)
          (let [message (protocol/read-completion buffer offset-rest)
                completion-id (:payload message)]
            (>!! (:release-ch (get @virtual-peers (:peer-id message)))
                 completion-id))

          (= msg-type protocol/retry-msg-id)
          (let [message (protocol/read-retry buffer offset-rest)
                retry-id (:payload message)]
            (>!! (:retry-ch (get @virtual-peers (:peer-id message))) 
                 retry-id)))))

(defn start-subscriber! [bind-addr port virtual-peers decompress-f]
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        aeron (Aeron/connect ctx)
        channel (aeron-channel bind-addr port)
        handler (fragment-data-handler 
                  (fn [buffer offset length header] 
                    (handle-message decompress-f virtual-peers
                                    buffer offset length header)))

        subscriber (.addSubscription aeron channel send-stream-id)
        receive-idle-strategy (backoff-strategy :high-restart-latency)
        subscriber-fut (future (try (.accept ^Consumer (consumer handler receive-idle-strategy 10) subscriber) 
                                    (catch Throwable e (fatal e))))]
    {:subscriber subscriber :subscriber-fut subscriber-fut}))

;; FIXME, race condition 
(defn get-publication [publications channel]
  (if-let [pub (get @publications channel)]
    pub
    (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
          aeron (Aeron/connect ctx)
          pub (.addPublication aeron channel send-stream-id)]
      (do (swap! publications assoc channel pub)
          pub))))

(defn opts->port [opts]
  (or (first (common/allowable-ports opts))
      (throw 
        (ex-info "Couldn't assign port - ran out of available ports.
                 Available ports can be configured in the peer-config.
                 e.g. {:onyx.messaging/peer-ports [40000, 40002],
                 :onyx.messaging/peer-port-range [40200 40260]}"))))

(defrecord AeronPeerGroup [opts publications compress-f decompress-f send-idle-strategy]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (if-not (nil? (:onyx.messaging.aeron/embedded-driver? opts))
                             (:onyx.messaging.aeron/embedded-driver? opts)
                             (:onyx.messaging.aeron/embedded-driver? defaults))
          
          media-driver (if embedded-driver?
                         (MediaDriver/launch 
                           (doto (MediaDriver$Context.) 
                             #_(.threadingMode ThreadingMode/DEDICATED)
                             #_(.dirsDeleteOnExit true))))

          port (opts->port opts)
          bind-addr (common/bind-addr opts)
          external-addr (common/external-addr opts)
          idle-strategy-config (arg-or-default :onyx.messaging.aeron/idle-strategy opts) 
          send-idle-strategy (backoff-strategy idle-strategy-config)
          receive-idle-strategy (backoff-strategy idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn opts) compress)
          decompress-f (or (:onyx.messaging/decompress-fn opts) decompress)
          virtual-peers (atom {})
          publications (atom {})
          subscriber (start-subscriber! bind-addr port virtual-peers decompress-f)]
      (assoc component 
             :bind-addr bind-addr
             :external-addr external-addr
             :port port
             :media-driver media-driver 
             :publications publications
             :virtual-peers virtual-peers
             :compress-f compress-f
             :decompress-f decompress-f
             :port port
             :send-idle-strategy send-idle-strategy
             :subscriber subscriber)))

  (stop [{:keys [media-driver subscriber] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (future-cancel (:subscriber-fut subscriber))
    (when media-driver (.close ^MediaDriver media-driver))
    (assoc component :media-driver nil :publications nil :virtual-peers nil
           :compress-f nil :decompress-f nil :send-idle-strategy nil
           :subscriber nil)))

(defn aeron-peer-group [opts]
  (map->AeronPeerGroup {:opts opts}))

(defmethod extensions/assign-site-resources :aeron
  [replica peer-site peer-sites]
  {})

(defmethod extensions/get-peer-site :aeron
  [replica peer]
  (get-in replica [:peer-sites peer :aeron/external-addr]))

(defn aeron [peer-group]
  (map->AeronConnection {:peer-group peer-group}))

(defmethod extensions/peer-site AeronConnection
  [messenger]
  {:aeron/external-addr (:external-addr (:messaging-group messenger))
   :aeron/port (:port (:messaging-group messenger))})

;;;; THIS CAN BE MADE FASTER BY NOT GOING THROUGH PEER-LINK IN OPERATION
(defmethod extensions/connect-to-peer AeronConnection
  [messenger peer-id event {:keys [aeron/external-addr aeron/port]}]
  (->AeronPeerConnection (aeron-channel external-addr port) peer-id))

(defmethod extensions/receive-messages AeronConnection
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ch (:inbound-ch messenger)
        batch-size (long (:onyx/batch-size task-map))
        ms (arg-or-default :onyx/batch-timeout task-map)
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i batch-size)
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defmethod extensions/send-messages AeronConnection
  [messenger event peer-link batch]
  (let [pub ^uk.co.real_logic.aeron.Publication (get-publication (:publications messenger) 
                                                                 (:channel peer-link))
        peer-id (:peer-id peer-link)
        [len unsafe-buffer] (protocol/build-messages-msg-buf (:compress-f messenger) peer-id batch)
        offer-f (fn [] (.offer pub unsafe-buffer 0 len))
        idle-strategy (:send-idle-strategy messenger)]
    ;;; TODO: offer will return a particular code when the publisher is down
    ;;; we should probably re-restablish the publication in this case
    (while (neg? ^long (offer-f))
      (.idle ^IdleStrategy idle-strategy 0))))

(defmethod extensions/internal-ack-messages AeronConnection
  [messenger event peer-link acks]
  ; TODO: Might want to batch in a single buffer as in netty
  (let [pub ^uk.co.real_logic.aeron.Publication (get-publication (:publications messenger) 
                                                                 (:channel peer-link))
        peer-id (:peer-id peer-link)
        idle-strategy (:send-idle-strategy messenger)] 
    (doseq [{:keys [id completion-id ack-val]} acks] 
      (let [unsafe-buffer (protocol/build-acker-message peer-id id completion-id ack-val)
            offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/ack-msg-length))]
        (while (neg? ^long (offer-f))
          (.idle ^IdleStrategy idle-strategy 0))))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event id peer-link]
  (let [idle-strategy (:send-idle-strategy messenger)
        pub ^uk.co.real_logic.aeron.Publication (get-publication (:publications messenger) 
                                                                 (:channel peer-link))
        peer-id (:peer-id peer-link)
        unsafe-buffer (protocol/build-completion-msg-buf peer-id id)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/completion-msg-length))]
    (while (neg? ^long (offer-f))
      (.idle ^IdleStrategy idle-strategy 0))))

(defmethod extensions/internal-retry-message AeronConnection
  [messenger event id peer-link]
  (let [idle-strategy (:send-idle-strategy messenger)
        pub ^uk.co.real_logic.aeron.Publication (get-publication (:publications messenger) 
                                                                 (:channel peer-link))
        peer-id (:peer-id peer-link)
        unsafe-buffer (protocol/build-retry-msg-buf peer-id id)
        offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/retry-msg-length))]
    (while (neg? ^long (offer-f))
      (.idle ^IdleStrategy idle-strategy 0))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  {})
