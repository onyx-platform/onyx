(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.aeron.publication-manager :as pubm]
            [onyx.messaging.aeron.publication-pool :as pub-pool :refer [get-publication]]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :refer [->MonitorEventBytes map->Barrier]]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

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

(defrecord AeronMessenger
  [peer-group messaging-group publication-group short-circuitable? publications virtual-peers 
   send-idle-strategy compress-f monitoring publication-pool short-ids acking-ch]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron Messenger")
    (let [config (:config peer-group)
          messaging-group (:messaging-group peer-group)
          publication-pool (:publication-pool messaging-group)
          publications (:publications messaging-group)
          virtual-peers (:virtual-peers messaging-group)
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f (:messaging-group peer-group))
          short-ids (atom {})]
      (assoc component
             :messaging-group messaging-group
             :publication-pool publication-pool
             :short-ids short-ids
             :virtual-peers virtual-peers
             :send-idle-strategy send-idle-strategy
             :compress-f compress-f)))

  (stop [{:keys [retry-ch publication-pool virtual-peers short-ids acker-short-id] :as component}]
    (taoensso.timbre/info "Stopping Aeron Messenger")
    (try
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
           :compress-f nil)))

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
          poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts)
          offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy opts)
          send-idle-strategy (backoff-strategy poll-idle-strategy-config)
          receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn opts) messaging-compress)
          decompress-f (or (:onyx.messaging/decompress-fn opts) messaging-decompress)
          virtual-peers (atom (pm/vpeer-manager))
          publication-pool (component/start (pub-pool/new-publication-pool opts send-idle-strategy))]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (fn [] 
                                     (.deleteAeronDirectory ^MediaDriver$Context media-driver-context)))))
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :media-driver-context media-driver-context
             :media-driver media-driver
             :publication-pool publication-pool
             :virtual-peers virtual-peers
             :compress-f compress-f
             :decompress-f decompress-f
             :port port
             :send-idle-strategy send-idle-strategy)))

  (stop [{:keys [media-driver media-driver-context subscribers publication-pool] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop publication-pool)

    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :bind-addr nil :external-addr nil
           :media-driver nil :publications nil :virtual-peers nil
           :media-driver-context nil
           :external-channel nil
           :compress-f nil :decompress-f nil :send-idle-strategy nil)))

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

(defn aeron-messenger [peer-group]
  (map->AeronMessenger {:peer-group peer-group}))

(defmethod extensions/peer-site AeronMessenger
  [messenger]
  {:aeron/external-addr (:external-addr (:messaging-group messenger))
   :aeron/port (:port (:messaging-group messenger))})

(defrecord AeronPeerConnection [channel stream-id acker-id peer-task-id])

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defmethod extensions/connection-spec AeronMessenger
  [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/acker-id aeron/peer-task-id] :as peer-site}]
  (let [sub-count (:subscriber-count (:messaging-group messenger))
        ;; ensure that each machine spreads their use of a node/peer-group's
        ;; streams evenly over the cluster
        stream-id 1]
    (->AeronPeerConnection (aeron-channel external-addr port) stream-id acker-id peer-task-id)))

(defn handle-message
  [result-state this-task-id upstream-task-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes buffer offset ba)
        res (messaging-decompress ba)]
    (cond (and (record? res) (= this-task-id (:dst-task res)))
          (do (swap! result-state conj (map->Barrier (into {} res)))
              ControlledFragmentHandler$Action/BREAK)

          (and (record? res) (not= this-task-id (:dst-task res)))
          ControlledFragmentHandler$Action/CONTINUE

          (coll? res)
          (do (doseq [m res]
                (when (and (= (:dst-task m) this-task-id)
                           (= (:src-task m) upstream-task-id))
                  (swap! result-state conj m)))
              ControlledFragmentHandler$Action/CONTINUE)

          (:barrier-id res)
          (comment "pass")

          :else (throw (ex-info "Not sure what happened" {})))))

(defn fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn rotate [xs]
  (conj (into [] (rest xs)) (first xs)))

(defmethod extensions/receive-messages AeronMessenger
  [messenger {:keys [onyx.core/subscriptions onyx.core/task-map
                     onyx.core/messenger-buffer onyx.core/subscription-maps]
              :as event}]
  (let [next-subscription (first (swap! subscription-maps rotate))
        subscription (:subscription next-subscription)
        result-state (atom [])
        fh (fragment-data-handler (partial handle-message result-state (:onyx.core/task-id event)
                                           (:upstream-task next-subscription)))
        n-fragments (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh 10)]
    @result-state))

(defn lookup-channels [messenger id]
  (-> messenger
      :virtual-peers
      deref
      (pm/peer-channels id)))

(defmethod extensions/send-messages AeronMessenger
  [messenger {:keys [peer-task-id channel] :as conn-spec} batch]
  (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
        buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress batch))]
    (pubm/write pub-man buf 0 (.capacity buf))))

(defmethod extensions/send-barrier AeronMessenger
  [messenger {:keys [peer-task-id channel] :as conn-spec} barrier]
  (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
        buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress barrier))]
    (pubm/write pub-man buf 0 (.capacity buf))))

(defmethod extensions/internal-complete-segment AeronMessenger
  [messenger completion-id {:keys [peer-task-id channel] :as conn-spec}]
  (let [pub-man (get-publication (:publication-pool messenger) conn-spec)
        buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress completion-id))]
    (pubm/write pub-man buf 0 (.capacity buf))))
