(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.aeron.publication-manager :as pubm]
            [onyx.messaging.aeron.publication-pool :as pub-pool :refer [get-publication]]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :refer [->MonitorEventBytes]]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context FragmentAssembler Publication Subscription]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defrecord AeronMessenger
  [messaging-group peer-config monitoring publication-group short-circuitable? publications virtual-peers 
   acking-daemon send-idle-strategy compress-f publication-pool short-ids acking-ch]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron Messenger")
    (let [publication-pool (:publication-pool messaging-group)
          publications (:publications messaging-group)
          virtual-peers (:virtual-peers messaging-group)
          external-channel (:external-channel messaging-group)
          short-circuitable? (if (arg-or-default :onyx.messaging/allow-short-circuit? peer-config)
                               (fn [channel] (= channel external-channel))
                               (fn [_] false))
          acking-ch (:acking-ch (:acking-daemon component))
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f messaging-group)
          short-ids (atom {})]
      (assoc component
             :messaging-group messaging-group
             :short-circuitable? short-circuitable?
             :publication-pool publication-pool
             :short-ids short-ids
             :virtual-peers virtual-peers
             :send-idle-strategy send-idle-strategy
             :compress-f compress-f
             :acking-ch acking-ch)))

  (stop [{:keys [retry-ch acking-ch publication-pool virtual-peers short-ids acker-short-id] :as component}]
    (taoensso.timbre/info "Stopping Aeron Messenger")
    (try
      (close! acking-ch)
      (let [short-ids-snapshot @short-ids]
        (when (:peer-task-short-id short-ids-snapshot)
          (swap! virtual-peers pm/dissoc (:peer-task-short-id short-ids-snapshot)))
        (when (:acker-short-id short-ids-snapshot)
          (swap! virtual-peers pm/dissoc (:acker-short-id short-ids-snapshot))))
      (catch Throwable e (fatal e)))
    (assoc component
           :send-idle-strategy nil
           :short-circuitable? nil
           :publication-pool nil
           :virtual-peers nil
           :short-ids nil
           :compress-f nil
           :acking-ch nil)))

(defmethod extensions/register-acker AeronMessenger
  [{:keys [virtual-peers short-ids acking-ch] :as messenger}
   {:keys [aeron/acker-id]}]
  (swap! short-ids assoc :acker-short-id acker-id)
  (swap! virtual-peers pm/assoc acker-id acking-ch))

(defmethod extensions/register-task-peer AeronMessenger
  [{:keys [virtual-peers short-ids] :as messenger}
   {:keys [aeron/peer-task-id]}
   task-buffer]
  (swap! short-ids assoc :peer-task-short-id peer-task-id)
  (swap! virtual-peers pm/assoc peer-task-id task-buffer))

(defmethod extensions/unregister-task-peer AeronMessenger
  [{:keys [virtual-peers short-ids] :as messenger}
   {:keys [aeron/peer-task-id]}]
  (swap! short-ids dissoc peer-task-id)
  (when peer-task-id 
    (swap! virtual-peers pm/dissoc peer-task-id)))

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
        offset (unchecked-add offset protocol/short-size)
        v-peers @virtual-peers]
    (cond (= msg-type protocol/ack-msg-id)
          (let [ack (protocol/read-acker-message buffer offset)]
            (when-let [acking-ch (pm/peer-channels v-peers peer-id)]
              (>!! acking-ch ack)))

          (= msg-type protocol/batched-ack-msg-id)
          (let [acks (protocol/read-acker-messages buffer offset)]
            (when-let [acking-ch (pm/peer-channels v-peers peer-id)]
              (run! (fn [ack]
                      (>!! acking-ch ack))
                    acks))) 

          (= msg-type protocol/messages-msg-id)
          (let [segments (protocol/read-messages-buf decompress-f buffer offset)]
            (when-let [chs (pm/peer-channels v-peers peer-id)]
              (let [inbound-ch (:inbound-ch chs)]
                (run! (fn [segment]
                        (>!! inbound-ch segment))
                      segments))))

          (= msg-type protocol/completion-msg-id)
          (let [completion-id (protocol/read-completion buffer offset)]
            (when-let [chs (pm/peer-channels v-peers peer-id)]
              (>!! (:release-ch chs) completion-id)))

          (= msg-type protocol/retry-msg-id)
          (let [retry-id (protocol/read-retry buffer offset)]
            (when-let [chs (pm/peer-channels v-peers peer-id)]
              (>!! (:retry-ch chs) retry-id)))

          :else
          (throw (ex-info "Could not deserialize incoming message from Aeron"
                          {:msg-type msg-type})))))

(defn start-subscriber! [bind-addr port stream-id virtual-peers decompress-f idle-strategy]
  (let [ctx (-> (Aeron$Context.)
                (.errorHandler no-op-error-handler))
        conn (Aeron/connect ctx)
        channel (aeron-channel bind-addr port)
        handler (fragment-data-handler
                  (fn [buffer offset length header]
                    (handle-message decompress-f virtual-peers buffer offset length header)))
        subscription (.addSubscription conn channel stream-id)
        subscriber-fut (future (try (.accept ^Consumer (consumer handler idle-strategy 10) subscription)
                                    (catch Throwable e (fatal e))))]
    {:conn conn :subscription subscription :subscriber-fut subscriber-fut}))

(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defrecord AeronPeerGroup [opts publication-pool subscribers subscriber-count compress-f decompress-f send-idle-strategy]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? opts)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading opts))

          media-driver-context (if embedded-driver?
                                 (-> (MediaDriver$Context.) 
                                     (.threadingMode threading-mode)
                                     (.dirsDeleteOnStart true)))

          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))

          bind-addr (common/bind-addr opts)
          external-addr (common/external-addr opts)
          port (:onyx.messaging/peer-port opts)
          external-channel (aeron-channel external-addr port)
          poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts)
          offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy opts)
          send-idle-strategy (backoff-strategy poll-idle-strategy-config)
          receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn opts) messaging-compress)
          decompress-f (or (:onyx.messaging/decompress-fn opts) messaging-decompress)
          virtual-peers (atom (pm/vpeer-manager))
          publication-pool (component/start (pub-pool/new-publication-pool opts send-idle-strategy))
          subscriber-count (arg-or-default :onyx.messaging.aeron/subscriber-count opts)
          subscribers (mapv (fn [stream-id]
                              (start-subscriber! bind-addr port stream-id virtual-peers decompress-f receive-idle-strategy))
                            (range subscriber-count))]
      (when embedded-driver?
        (.addShutdownHook (Runtime/getRuntime)
                          (Thread.
                           (fn []
                             (try
                               (.deleteAeronDirectory ^MediaDriver$Context media-driver-context)
                               (catch java.nio.file.NoSuchFileException nfe))))))
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :external-channel external-channel
             :media-driver-context media-driver-context
             :media-driver media-driver
             :publication-pool publication-pool
             :virtual-peers virtual-peers
             :compress-f compress-f
             :decompress-f decompress-f
             :port port
             :send-idle-strategy send-idle-strategy
             :subscriber-count subscriber-count
             :subscribers subscribers)))

  (stop [{:keys [media-driver media-driver-context subscribers publication-pool] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop publication-pool)
    (doseq [subscriber subscribers]
      (future-cancel (:subscriber-fut subscriber))
      (.close ^Subscription (:subscription subscriber))
      (.close ^Aeron (:conn subscriber)))
     
    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :bind-addr nil :external-addr nil :external-channel nil
           :media-driver nil :publications nil :virtual-peers nil
           :media-driver-context nil
           :external-channel nil
           :subscriber-count nil
           :compress-f nil :decompress-f nil :send-idle-strategy nil
           :subscribers nil)))

(defmethod clojure.core/print-method AeronPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))

(defn aeron-peer-group [opts]
  (map->AeronPeerGroup {:opts opts}))

(def possible-ids
  (set (map short (range -32768 32768))))

(defn available-ids [used]
  (clojure.set/difference possible-ids used))

(defn choose-id [hsh used]
  (when-let [available (available-ids used)]
    (nth (seq available) (mod hsh (count available)))))

(defn allocate-id [peer-id peer-site peer-sites]
  ;;; Assigns a unique id to each peer so that messages do not need
  ;;; to send the entire peer-id in a payload, saving 14 bytes per
  ;;; message
  (let [used-ids (->> (vals peer-sites)
                      (filter
                        (fn [s]
                          (= (:aeron/external-addr peer-site)
                             (:aeron/external-addr s))))
                      (mapcat (juxt :aeron/acker-id :aeron/peer-id))
                      set)
        id (choose-id peer-id used-ids)]
    (when-not id
      (throw (ex-info "Couldn't assign id. Ran out of aeron ids. 
                      This should only happen if more than 65356 virtual peers have been started up on a single external addr."
                      peer-site)))
    id))

(defmethod extensions/assign-acker-resources :aeron
  [replica peer-id peer-site peer-sites]
  {:aeron/acker-id (allocate-id (hash peer-id) peer-site peer-sites)})

(defmethod extensions/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod extensions/get-peer-site :aeron
  [replica peer]
  (get-in replica [:peer-sites peer :aeron/external-addr]))

(defn aeron-messenger [peer-config messaging-group]
  (map->AeronMessenger {:peer-config peer-config :messaging-group messaging-group}))

(defmethod extensions/peer-site AeronMessenger
  [messenger]
  {:aeron/external-addr (:external-addr (:messaging-group messenger))
   :aeron/port (:port (:messaging-group messenger))})

(defrecord AeronPeerConnection [channel stream-id acker-id peer-task-id])

(defmethod extensions/connection-spec AeronMessenger
  [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/acker-id aeron/peer-task-id] :as peer-site}]
  (let [sub-count (:subscriber-count (:messaging-group messenger))
        ;; ensure that each machine spreads their use of a node/peer-group's
        ;; streams evenly over the cluster
        stream-id (mod (hash (str external-addr
                                  (:external-addr (:messaging-group messenger))))
                       sub-count)]
    (->AeronPeerConnection (aeron-channel external-addr port) stream-id acker-id peer-task-id)))

(defmethod extensions/receive-messages AeronMessenger
  [messenger {:keys [onyx.core/task-map onyx.core/messenger-buffer] :as event}]
  ;; We reuse a single timeout channel. This allows us to
  ;; continually block against one thread that is continually
  ;; expiring. This property lets us take variable amounts of
  ;; time when reading each segment and still allows us to return
  ;; within the predefined batch timeout limit.
  (let [ch (:inbound-ch messenger-buffer)
        batch-size (long (:onyx/batch-size task-map))
        ms (arg-or-default :onyx/batch-timeout task-map)
        timeout-ch (timeout ms)]
    (loop [segments (transient []) i 0]
      (if (< i batch-size)
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj! segments v) (inc i))
          (persistent! segments))
        (persistent! segments)))))

(defn lookup-channels [messenger id]
  (-> messenger
      :virtual-peers
      deref
      (pm/peer-channels id)))

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
    (run! (fn [ack]
            (>!! ch ack))
          acks)))

(defn complete-message-short-circuit [ch completion-id]
  (when ch
    (>!! ch completion-id)))

(defn retry-segment-short-circuit [ch retry-id]
  (when ch
    (>!! ch retry-id)))

(defmethod extensions/send-messages AeronMessenger
  [messenger {:keys [peer-task-id channel] :as conn-spec} batch]
  (if ((:short-circuitable? messenger) channel)
    (send-messages-short-circuit (:inbound-ch (lookup-channels messenger peer-task-id)) batch)
    (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
          buf ^UnsafeBuffer (protocol/build-messages-msg-buf (:compress-f messenger) peer-task-id batch)]
      (pubm/write pub-man buf 0 (.capacity buf)))))

(defmethod extensions/internal-ack-segment AeronMessenger
  [messenger {:keys [acker-id channel] :as conn-spec} ack]
  (if ((:short-circuitable? messenger) channel)
    (ack-segment-short-circuit (lookup-channels messenger acker-id) ack)
    (let [pub-man (get-publication (:publication-pool messenger) conn-spec)]
      (let [buf (protocol/build-acker-message acker-id (:id ack) (:completion-id ack) (:ack-val ack))]
        (pubm/write pub-man buf 0 protocol/ack-msg-length)))))

(defmethod extensions/internal-ack-segments AeronMessenger
  [messenger {:keys [acker-id channel] :as conn-spec} acks]
  (if ((:short-circuitable? messenger) channel)
    (ack-segments-short-circuit (lookup-channels messenger acker-id) acks)
    (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
          buf ^UnsafeBuffer (protocol/build-acker-messages acker-id acks)
          size (.capacity buf)]
      (extensions/emit (:monitoring messenger) (->MonitorEventBytes :peer-send-bytes size))
      (pubm/write pub-man buf 0 size))))

(defmethod extensions/internal-complete-segment AeronMessenger
  [messenger completion-id {:keys [peer-task-id channel] :as conn-spec}]
  (if ((:short-circuitable? messenger) channel)
    (complete-message-short-circuit (:release-ch (lookup-channels messenger peer-task-id)) completion-id)
    (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
          buf (protocol/build-completion-msg-buf peer-task-id completion-id)]
      (pubm/write pub-man buf 0 protocol/completion-msg-length))))

(defmethod extensions/internal-retry-segment AeronMessenger
  [messenger retry-id {:keys [peer-task-id channel] :as conn-spec}]
  (if ((:short-circuitable? messenger) channel)
    (retry-segment-short-circuit (:retry-ch (lookup-channels messenger peer-task-id)) retry-id)
    (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
          buf (protocol/build-retry-msg-buf peer-task-id retry-id)]
      (pubm/write pub-man buf 0 protocol/retry-msg-length))))
