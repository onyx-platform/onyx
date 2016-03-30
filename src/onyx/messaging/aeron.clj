(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :refer [->MonitorEventBytes map->Barrier]]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [uk.co.real_logic.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription FragmentAssembler]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(def no-op-error-handler
  (reify ErrorHandler
    (onError [this x] (taoensso.timbre/warn x))))

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

(defn global-stream-observer-handler [buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes buffer offset ba)
        res (messaging-decompress ba)]
;;    (info res)
    ))

(defn global-fragment-data-handler [f]
  (FragmentAssembler.
   (reify FragmentHandler
     (onFragment [this buffer offset length header]
       (f buffer offset length header)))))

(defn global-consumer [handler ^IdleStrategy idle-strategy limit]
  (reify Consumer
    (accept [this subscription]
      (while (not (Thread/interrupted))
        (let [fragments-read (.poll ^Subscription subscription ^FragmentHandler handler ^int limit)]
          (.idle idle-strategy fragments-read))))))

(defn start-global-stream-observer! [conn bind-addr port stream-id idle-strategy]
  (let [channel (aeron-channel bind-addr port)
        subscription (.addSubscription conn channel stream-id)
        handler (global-fragment-data-handler
                 (fn [buffer offset length header]
                   (global-stream-observer-handler buffer offset length header)))
        subscription-fut (future (try (.accept ^Consumer (global-consumer handler idle-strategy 10) subscription)
                                    (catch Throwable e (fatal e))))]
    {:subscription subscription
     :subscription-fut subscription-fut}))

(defrecord AeronMessenger
  [peer-group messaging-group publication-group publications
   send-idle-strategy compress-f monitoring short-ids acking-ch]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Aeron Messenger")
    (let [config (:config peer-group)
          messaging-group (:messaging-group peer-group)
          publications (atom {})
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f (:messaging-group peer-group))
          ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
          aeron-conn (Aeron/connect ctx)
          global-stream-observer (start-global-stream-observer!
                                  aeron-conn
                                  (mc/bind-addr (:config peer-group))
                                  (:onyx.messaging/peer-port (:config peer-group))
                                  1
                                  (backoff-strategy (arg-or-default :onyx.messaging.aeron/poll-idle-strategy (:config peer-group))))
          aeron-conn aeron-conn
          short-ids (atom {})]
      (assoc component
             :messaging-group messaging-group
             :short-ids short-ids
             :send-idle-strategy send-idle-strategy
             :publications publications
             :compress-f compress-f
             :aeron-conn aeron-conn
             :global-stream-observer global-stream-observer)))

  (stop [{:keys [short-ids publications] :as component}]
    (taoensso.timbre/info "Stopping Aeron Messenger")
    (run! #(.close %) (vals @publications))
    (future-cancel (:subscription-fut (:global-stream-observer component)))
    (.close ^Subscription (:subscription (:global-stream-observer component)))
    (.close ^Aeron (:aeron-conn component))

    (assoc component
           :messaging-group nil
           :send-idle-strategy nil
           :publications nil
           :short-ids nil
           :compress-f nil)))

(defmethod extensions/register-task-peer AeronMessenger
  [{:keys [short-ids] :as messenger}
   {:keys [aeron/peer-task-id]}
   task-buffer]
  (swap! short-ids assoc :peer-task-short-id peer-task-id))

(defmethod extensions/unregister-task-peer AeronMessenger
  [{:keys [short-ids] :as messenger}
   {:keys [aeron/peer-task-id]}]
  (swap! short-ids dissoc peer-task-id))

(def no-op-error-handler
  (reify ErrorHandler
    (onError [this x] (taoensso.timbre/warn x))))

(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defrecord AeronPeerGroup [opts subscribers subscriber-count compress-f decompress-f send-idle-strategy]
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
          decompress-f (or (:onyx.messaging/decompress-fn opts) messaging-decompress)]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (fn [] 
                                     (.deleteAeronDirectory ^MediaDriver$Context media-driver-context)))))
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :media-driver-context media-driver-context
             :media-driver media-driver
             :compress-f compress-f
             :decompress-f decompress-f
             :port port
             :send-idle-strategy send-idle-strategy)))

  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")

    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :bind-addr nil :external-addr nil
           :media-driver nil :media-driver-context nil :external-channel nil
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
                      (map :aeron/peer-id)
                      set)
        id (choose-id peer-id used-ids)]
    (when-not id
      (throw (ex-info "Couldn't assign id. Ran out of aeron ids. 
                      This should only happen if more than 65356 virtual peers have been started up on a single external addr."
                      peer-site)))
    id))

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

(defrecord AeronPeerConnection [channel stream-id peer-task-id])

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defmethod extensions/connection-spec AeronMessenger
  [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/peer-task-id] :as peer-site}]
  (let [sub-count (:subscriber-count (:messaging-group messenger))
        ;; ensure that each machine spreads their use of a node/peer-group's
        ;; streams evenly over the cluster
        stream-id 1]
    (->AeronPeerConnection (aeron-channel external-addr port) stream-id peer-task-id)))

(defn handle-message
  [result-state upstream-peer-id this-task-name buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes buffer offset ba)
        res (messaging-decompress ba)]
    (cond (and (instance? onyx.types.Barrier res)
               (= (:from-peer-id res) upstream-peer-id))
          (do (swap! result-state conj res)
              ControlledFragmentHandler$Action/BREAK)

          (and (instance? onyx.types.Leaf res)
               (= (:from-peer-id res) upstream-peer-id))
          (do (swap! result-state conj res)
              ControlledFragmentHandler$Action/CONTINUE)

          :else
          ControlledFragmentHandler$Action/CONTINUE)))

(defn fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn rotate [xs]
  (conj (into [] (rest xs)) (first xs)))

(defn remove-blocked-consumers
  "Implements barrier alignment"
  [replica job-id barrier-state subscription-maps]
  ;; There should be at most one barrier that we're tracking
  ;; since we are aligning.
  (assert (<= (count (keys barrier-state)) 1) (str "Was: " barrier-state))
  (remove
   (fn [{:keys [upstream-peer-id]}]
     (some #{upstream-peer-id} (:peers (get barrier-state (first (keys barrier-state))))))
   subscription-maps))

(def fragment-limit 10)

(defmethod extensions/receive-messages AeronMessenger
  [messenger {:keys [onyx.core/subscriptions onyx.core/task-map
                     onyx.core/replica onyx.core/job-id
                     onyx.core/barrier-state
                     onyx.core/messenger-buffer onyx.core/subscription-maps]
              :as event}]
  (let [rotated-subscriptions (swap! subscription-maps rotate)
        removed-subscriptions (remove-blocked-consumers @replica job-id @barrier-state rotated-subscriptions)
        next-subscription (first removed-subscriptions)]
    (if next-subscription
      (let [subscription (:subscription next-subscription)
            result-state (atom [])
            fh (fragment-data-handler (partial handle-message result-state (:upstream-peer-id next-subscription) (:onyx.core/task event)))
            n-fragments (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit)]
        @result-state)
      [])))

(defn get-publication [publications channel stream-id]
  (if-let [pub (get @publications [channel stream-id])]
    pub
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (taoensso.timbre/warn "Aeron messaging publication error:" x)))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler))
          conn (Aeron/connect ctx)
          pub (.addPublication conn channel stream-id)]
      (swap! publications assoc [channel stream-id] pub)
      pub)))

(defn write [^Publication pub ^UnsafeBuffer buf]
  ;; Needs an escape mechanism so it can break if a peer is shutdown
  ;; Needs an idle mechanism to prevent cpu burn
  (while (neg? (.offer pub buf 0 (.capacity buf)))
;;    (info "Re-offering message, session-id" (.sessionId pub))
    ))

(defmethod extensions/send-messages AeronMessenger
  [{:keys [publications]} {:keys [channel stream-id] :as conn-spec} batch]
  (let [pub (get-publication publications channel stream-id)]
    (doseq [b batch]
      (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress b))]
        (write pub buf)))))

(defmethod extensions/send-barrier AeronMessenger
  [{:keys [publications]} {:keys [channel stream-id] :as conn-spec} barrier]
  (let [pub (get-publication publications channel stream-id)
        buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress barrier))]
    (write pub buf)))

(defmethod extensions/internal-complete-segment AeronMessenger
  [{:keys [publications]} completion-message {:keys [channel stream-id] :as conn-spec}]
  (let [pub (get-publication publications channel stream-id)
        buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress completion-message))]
    (write pub buf)))
