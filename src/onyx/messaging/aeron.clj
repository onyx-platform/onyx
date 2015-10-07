(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.aeron.publication-manager :as pubm]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :refer [->MonitorEventBytes]]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription AvailableImageHandler]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler]
           [uk.co.real_logic.agrona ErrorHandler CloseHelper]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))


(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defrecord AeronConnection
  [peer-group messaging-group short-circuitable? publications virtual-peers acking-daemon acking-ch
   send-idle-strategy compress-f inbound-ch release-ch retry-ch]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron")
    (let [config (:config peer-group)
          messaging-group (:messaging-group peer-group)
          publications (:publications messaging-group)
          virtual-peers (:virtual-peers messaging-group)
          inbound-ch (:inbound-ch (:messenger-buffer component))
          external-channel (:external-channel messaging-group)
          short-circuitable? (if (arg-or-default :onyx.messaging/allow-short-circuit? config)
                               (fn [channel] (= channel external-channel))
                               (fn [_] false))
          release-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/release-ch-buffer-size config)))
          retry-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/retry-ch-buffer-size config)))
          write-buffer-size (arg-or-default :onyx.messaging.aeron/write-buffer-size config)
          acking-ch (:acking-ch (:acking-daemon component))
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f (:messaging-group peer-group))
          multiplex-id (atom nil)]
      (assoc component
             :messaging-group messaging-group
             :short-circuitable? short-circuitable?
             :publications publications
             :multiplex-id multiplex-id
             :virtual-peers virtual-peers
             :send-idle-strategy send-idle-strategy
             :write-buffer-size write-buffer-size
             :compress-f compress-f
             :acking-ch acking-ch
             :inbound-ch inbound-ch
             :release-ch release-ch
             :retry-ch retry-ch)))

  (stop [{:keys [release-ch retry-ch virtual-peers multiplex-id] :as component}]
    (taoensso.timbre/info "Stopping Aeron")
    (try
      (close! release-ch)
      (close! retry-ch)
      (when @multiplex-id
        (swap! virtual-peers pm/dissoc @multiplex-id))
      (catch Throwable e (fatal e)))
    (assoc component
           :send-idle-strategy nil
           :short-circuitable? nil
           :publications nil
           :write-buffer-size nil
           :virtual-peers nil
           :multiplex-id nil
           :compress-f nil :decompress-f nil
           :inbound-ch nil :release-ch nil :retry-ch nil )))


(defmethod extensions/open-peer-site AeronConnection
  [{:keys [virtual-peers multiplex-id acking-ch inbound-ch release-ch retry-ch] :as messenger}
   {:keys [aeron/id]}]
  (reset! multiplex-id id)
  (swap! virtual-peers pm/assoc id (pm/->PeerChannels acking-ch inbound-ch release-ch retry-ch)))

(def no-op-error-handler
  (reify ErrorHandler
    (onError [this x] (taoensso.timbre/warn x))))

(defn fragment-data-handler [f]
  (FragmentAssembler.
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
        (let [fragments-read (.poll ^Subscription subscription ^FragmentHandler handler ^int limit)]
          (.idle idle-strategy fragments-read))))))

(defn handle-message [decompress-f virtual-peers buffer offset length header]
  ;;; All de-serialization is now done in a single subscriber thread
  ;;; If a job is serialization heavy, additional subscriber threads can be created
  ;;; via peer-config :onyx.messaging.aeron/subscriber-count
  (let [msg-type (protocol/read-message-type buffer offset)
        offset (inc ^long offset)
        peer-id (protocol/read-vpeer-id buffer offset)
        offset (unchecked-add offset protocol/short-size)]
    (cond (= msg-type protocol/ack-msg-id)
          (let [ack (protocol/read-acker-message buffer offset)]
            (when-let [chs (pm/peer-channels @virtual-peers peer-id)]
              (>!! (:acking-ch chs) ack)))

          (= msg-type protocol/batched-ack-msg-id)
          (let [acks (protocol/read-acker-messages buffer offset)]
            (when-let [chs (pm/peer-channels @virtual-peers peer-id)]
              (let [acking-ch (:acking-ch chs)]
                (run! (fn [ack]
                        (>!! acking-ch ack))
                      acks)))) 

          (= msg-type protocol/messages-msg-id)
          (let [segments (protocol/read-messages-buf decompress-f buffer offset)]
            (when-let [chs (pm/peer-channels @virtual-peers peer-id)]
              (let [inbound-ch (:inbound-ch chs)]
                (run! (fn [segment]
                        (>!! inbound-ch segment))
                      segments))))

          (= msg-type protocol/completion-msg-id)
          (let [completion-id (protocol/read-completion buffer offset)]
            (when-let [chs (pm/peer-channels @virtual-peers peer-id)]
              (>!! (:release-ch chs) completion-id)))

          (= msg-type protocol/retry-msg-id)
          (let [retry-id (protocol/read-retry buffer offset)]
            (when-let [chs (pm/peer-channels @virtual-peers peer-id)]
              (>!! (:retry-ch chs) retry-id))))))

(defn start-subscriber! [bind-addr port stream-id virtual-peers decompress-f idle-strategy]
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        conn (Aeron/connect ctx)
        channel (aeron-channel bind-addr port)
        handler (fragment-data-handler
                  (fn [buffer offset length header]
                    (handle-message decompress-f virtual-peers
                                    buffer offset length header)))
        subscription (.addSubscription conn channel stream-id)
        subscriber-fut (future (try (.accept ^Consumer (consumer handler idle-strategy 10) subscription)
                                    (catch Throwable e (fatal e))))]
    {:conn conn :subscription subscription :subscriber-fut subscriber-fut}))

(defrecord TrackedPub [publication last-used])

(defn get-publication [messenger {:keys [channel] :as conn-info}]
  ;; FIXME, race condition may cause two publications to be created
  ;; same goes for operation/peer-link
  (if-let [pub (get @(:publications messenger) channel)]
    (do
      (reset! (:last-used pub) (System/currentTimeMillis))
      (:publication pub))
    (let [stream-id (:stream-id conn-info)
          pub-manager (-> (pubm/new-publication-manager channel 
                                                        stream-id 
                                                        (:send-idle-strategy messenger) 
                                                        (:write-buffer-size messenger)
                                                        (fn []
                                                          (swap! (:publications messenger) dissoc channel))) 
                          (pubm/connect) 
                          (pubm/start))]
      (swap! (:publications messenger) 
             assoc 
             channel 
             (->TrackedPub pub-manager (atom (System/currentTimeMillis))))
      pub-manager)))

(defn opts->port [opts]
  (or (first (common/allowable-ports opts))
      (throw
        (ex-info "Couldn't assign port - ran out of available ports.
                 Available ports can be configured in the peer-config.
                 e.g. {:onyx.messaging/peer-ports [40000, 40002],
                       :onyx.messaging/peer-port-range [40200 40260]}"
                 {:opts opts}))))


;; FIXME: gc'ing publications could be racy if a publication is grabbed
;; just as it is being gc'd - though it is unlikely
(defn gc-publications [publications opts]
  (let [interval (arg-or-default :onyx.messaging/peer-link-gc-interval opts)
        idle ^long (arg-or-default :onyx.messaging/peer-link-idle-timeout opts)]
    (loop []
      (try
        (Thread/sleep interval)
        (let [t (System/currentTimeMillis)
              snapshot @publications
              to-remove (map first
                             (filter (fn [[k v]] (>= (- t ^long @(:last-used v)) idle))
                                     snapshot))]
          (doseq [k to-remove]
            (pubm/stop (:publication (snapshot k)))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (fatal e)))
      (recur))))

(defrecord AeronPeerGroup [opts publications subscribers subscriber-count compress-f decompress-f send-idle-strategy]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? opts)
          media-driver-context (if embedded-driver?
                                 (doto (MediaDriver$Context.)))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))

          bind-addr (common/bind-addr opts)
          external-addr (common/external-addr opts)
          port (opts->port opts)
          external-channel (aeron-channel external-addr port)
          poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts)
          offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy opts)
          send-idle-strategy (backoff-strategy poll-idle-strategy-config)
          receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn opts) compress)
          decompress-f (or (:onyx.messaging/decompress-fn opts) decompress)
          virtual-peers (atom (pm/vpeer-manager))
          publications (atom {})
          subscriber-count (arg-or-default :onyx.messaging.aeron/subscriber-count opts)
          subscribers (mapv (fn [stream-id]
                              (start-subscriber! bind-addr port stream-id virtual-peers decompress-f receive-idle-strategy))
                            (range subscriber-count))
          pub-gc-thread (future (gc-publications publications opts))]
      (assoc component
             :pub-gc-thread pub-gc-thread
             :bind-addr bind-addr
             :external-addr external-addr
             :external-channel external-channel
             :port port
             :media-driver-context media-driver-context
             :media-driver media-driver
             :publications publications
             :virtual-peers virtual-peers
             :compress-f compress-f
             :decompress-f decompress-f
             :port port
             :send-idle-strategy send-idle-strategy
             :subscriber-count subscriber-count
             :subscribers subscribers)))

  (stop [{:keys [media-driver media-driver-context subscribers publications] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (future-cancel (:pub-gc-thread component))
    (doseq [subscriber subscribers]
      (future-cancel (:subscriber-fut subscriber))
      (.close ^Subscription (:subscription subscriber))
      (.close ^Aeron (:conn subscriber)))
    (doseq [pub (vals @publications)]
      (pubm/stop (:publication pub))) 
    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :pub-gc-thread nil
           :bind-addr nil :external-addr nil :external-channel nil
           :media-driver nil :publications nil :virtual-peers nil
           :media-driver-context nil
           :external-channel nil
           :subscriber-count nil
           :compress-f nil :decompress-f nil :send-idle-strategy nil
           :subscribers nil)))

(defn aeron-peer-group [opts]
  (map->AeronPeerGroup {:opts opts}))

(def possible-ids
  (set (map short (range -32768 32768))))

(defn available-ids [used]
  (->> (clojure.set/difference possible-ids used)
       (into [])
       not-empty))

(defn choose-id [peer-id used]
  (when-let [available (available-ids used)]
    (nth available (mod (hash peer-id) (count available)))))

(defmethod extensions/assign-site-resources :aeron
  [replica peer-id peer-site peer-sites]
  ;;; Assigns a unique id to each peer so that messages do not need
  ;;; to send the entire peer-id in a payload, saving 14 bytes per
  ;;; message
  (let [used-ids (->> (vals peer-sites)
                      (filter
                        (fn [s]
                          (= (:aeron/external-addr peer-site)
                             (:aeron/external-addr s))))
                      (map :aeron/id)
                      set)
        id (choose-id peer-id used-ids)]
    (when-not id
      (throw (ex-info "Couldn't assign id. Ran out of aeron ids. This should only happen if more than 65356 virtual peers have been started up on a single external addr"
                      peer-site)))
    {:aeron/id id}))

(defmethod extensions/get-peer-site :aeron
  [replica peer]
  (get-in replica [:peer-sites peer :aeron/external-addr]))

(defn aeron [peer-group]
  (map->AeronConnection {:peer-group peer-group}))

(defmethod extensions/peer-site AeronConnection
  [messenger]
  {:aeron/external-addr (:external-addr (:messaging-group messenger))
   :aeron/port (:port (:messaging-group messenger))})

(defrecord AeronPeerConnection [channel stream-id id])

(defmethod extensions/connect-to-peer AeronConnection
  [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/id] :as peer-site}]
  (let [sub-count (:subscriber-count (:messaging-group messenger))
        ;; ensure that each machine spreads their use of a node/peer-group's
        ;; streams evenly over the cluster
        stream-id (mod (hash (str external-addr
                                  (:external-addr (:messaging-group messenger))))
                       sub-count)]
    (->AeronPeerConnection (aeron-channel external-addr port) stream-id id)))

(defmethod extensions/receive-messages AeronConnection
  [messenger {:keys [onyx.core/task-map] :as event}]
  ;; We reuse a single timeout channel. This allows us to
  ;; continually block against one thread that is continually
  ;; expiring. This property lets us take variable amounts of
  ;; time when reading each segment and still allows us to return
  ;; within the predefined batch timeout limit.
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

(defn short-circuit-ch [messenger id ch-k]
  (-> messenger
      :virtual-peers
      deref
      (pm/peer-channels id)
      ch-k))

(defn send-messages-short-circuit [ch batch]
  (when ch
    (run! (fn [segment]
            (>!! ch segment))
          batch)))

(defn ack-segment-short-circuit [ch ack]
  (when ch
    (>!! ch ack))) 

(defn ack-segments-short-circuit [ch acks]
  (when ch
    (doseq [ack acks]
      (>!! ch ack))))

(defn complete-message-short-circuit [ch completion-id]
  (when ch
    (>!! ch completion-id)))

(defn retry-segment-short-circuit [ch retry-id]
  (when ch
    (>!! ch retry-id)))

(defmethod extensions/send-messages AeronConnection
  [messenger event {:keys [id channel] :as conn-info} batch]
  (if ((:short-circuitable? messenger) channel)
    (send-messages-short-circuit (short-circuit-ch messenger (:id conn-info) :inbound-ch) batch)
    (let [pub-man (get-publication messenger conn-info)
          buf ^UnsafeBuffer (protocol/build-messages-msg-buf (:compress-f messenger) id batch)]
      (pubm/write pub-man buf 0 (.capacity buf)))))

(defmethod extensions/internal-ack-segment AeronConnection
  [messenger event {:keys [id channel] :as conn-info} ack]
  (if ((:short-circuitable? messenger) channel)
    (ack-segment-short-circuit (short-circuit-ch messenger id :acking-ch) ack)
    (let [pub-man (get-publication messenger conn-info)]
      (let [buf (protocol/build-acker-message id (:id ack) (:completion-id ack) (:ack-val ack))]
        (pubm/write pub-man buf 0 protocol/ack-msg-length)))))

(defmethod extensions/internal-ack-segments AeronConnection
  [messenger event conn-info acks]
  (if ((:short-circuitable? messenger) (:channel conn-info))
    (ack-segments-short-circuit (short-circuit-ch messenger (:id conn-info) :acking-ch) acks)
    (let [pub-man (get-publication messenger conn-info)
          buf ^UnsafeBuffer (protocol/build-acker-messages (:id conn-info) acks)
          size (.capacity buf)]
      (extensions/emit (:onyx.core/monitoring event) (->MonitorEventBytes :peer-send-bytes size))
      (pubm/write pub-man buf 0 size))))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event completion-id {:keys [id channel] :as conn-info}]
  (if ((:short-circuitable? messenger) channel)
    (complete-message-short-circuit (short-circuit-ch messenger id :release-ch) completion-id)
    (let [pub-man (get-publication messenger conn-info)
          buf (protocol/build-completion-msg-buf id completion-id)]
      (pubm/write pub-man buf 0 protocol/completion-msg-length))))

(defmethod extensions/internal-retry-segment AeronConnection
  [messenger event retry-id {:keys [id channel] :as conn-info}]
  (if ((:short-circuitable? messenger) channel)
    (retry-segment-short-circuit (short-circuit-ch messenger id :retry-ch) retry-id)
    (let [idle-strategy (:send-idle-strategy messenger)
          pub-man (get-publication messenger conn-info)
          buf (protocol/build-retry-msg-buf id retry-id)]
      (pubm/write pub-man buf 0 protocol/retry-msg-length))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  {})
