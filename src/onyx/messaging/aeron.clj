(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :refer [->MonitorEventBytes map->Barrier]]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(def fragment-limit-receiver 10)
(def global-fragment-limit 10)

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
          compress-f (:compress-f messaging-group)
          short-ids (atom {})]
      (assoc component
             :messaging-group messaging-group
             :short-ids short-ids
             :send-idle-strategy send-idle-strategy
             :publications publications
             :compress-f compress-f)))

  (stop [{:keys [short-ids publications] :as component}]
    (taoensso.timbre/info "Stopping Aeron Messenger")
    (run! (fn [{:keys [pub conn]}] 
            (.close pub)
            (.close conn)) 
          (vals @publications))

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
  #_(swap! short-ids assoc :peer-task-short-id peer-task-id))

(defmethod extensions/unregister-task-peer AeronMessenger
  [{:keys [short-ids] :as messenger}
   {:keys [aeron/peer-task-id]}]
  #_(swap! short-ids dissoc peer-task-id))

(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defrecord AeronPeerGroup [opts subscribers ticketing-counters compress-f decompress-f send-idle-strategy]
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
          ticketing-counters (atom {})
          ctx (.errorHandler (Aeron$Context.) no-op-error-handler)]
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
             :ticketing-counters ticketing-counters
             :port port
             :send-idle-strategy send-idle-strategy)))

  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")

    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :bind-addr nil :external-addr nil :media-driver nil :media-driver-context nil 
           :external-channel nil :compress-f nil :decompress-f nil :ticketing-counters nil 
           :send-idle-strategy nil)))

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
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

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

;; Define stream-id as only allowed stream
(def stream-id 1)

; (defmethod extensions/connection-spec AeronMessenger
;   [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/peer-task-id] :as peer-site}]
;   (->AeronPeerConnection (mc/aeron-channel external-addr port) stream-id peer-task-id))

(defmethod extensions/shared-ticketing-counter AeronMessenger
  [messenger job-id peer-id task-id]
  (let [path [job-id task-id peer-id]] 
    (get-in (swap! (:ticketing-counters (:messaging-group messenger)) 
                   (fn [tc]
                     (if (get-in tc path)
                        tc
                       (assoc-in tc path (atom 0)))))
            path)))

(defmethod extensions/new-partial-subscriber AeronMessenger
  [{:keys [messaging-group] :as messenger} job-id peer-id task-id]
  (info "new subscriber for " job-id peer-id task-id)
  (let [error-handler (reify ErrorHandler
                        (onError [this x] 
                          (taoensso.timbre/warn "Aeron messaging subscriber error:" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        bind-addr (:bind-addr messaging-group)
        port (:port messaging-group)
        channel (mc/aeron-channel bind-addr port)
        subscription (.addSubscription conn channel stream-id)]
    {:subscription subscription
     :conn conn
     :counter (atom 0)
     :ticket-counter (extensions/shared-ticketing-counter messenger job-id peer-id task-id)
     :barrier (atom nil)
     :src-peer-id peer-id}))

(defmethod extensions/close-partial-subscriber AeronMessenger
  [{:keys [messaging-group] :as messenger} partial-subscriber]
  (info "Closing partial subscriber")
  (.close ^Subscription (:subscription partial-subscriber))
  (.close ^Aeron (:conn partial-subscriber)))

(defn handle-message
  [barrier results subscriber-counter ticket-counter this-peer-id this-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes buffer offset ba)
        message (messaging-decompress ba)
        n-desired-messages 2]
    ;(info "handling message " (into {} message))
    (cond (>= (count @results) n-desired-messages)
          ControlledFragmentHandler$Action/ABORT
          (and (= (:dst-task-id message) this-task-id)
               (= (:src-peer-id message) src-peer-id))
          (cond (instance? onyx.types.Leaf message)
                (let [message-id @subscriber-counter 
                      ticket-id @ticket-counter]
                  (swap! subscriber-counter inc)
                  (cond (< message-id ticket-id)
                        ControlledFragmentHandler$Action/CONTINUE
                        (= message-id ticket-id)
                        (do (when (compare-and-set! ticket-counter ticket-id (inc ticket-id))
                              (swap! results conj message))
                            ControlledFragmentHandler$Action/CONTINUE)
                        (> message-id ticket-id)
                        (throw (ex-info "Shouldn't be possible to get ahead of a ticket id " {:message-id message-id :ticket-id ticket-id}))))
                (instance? onyx.types.Barrier message)
                (if (empty? @results)
                  (do (reset! barrier message)
                      ControlledFragmentHandler$Action/BREAK)  
                  ControlledFragmentHandler$Action/ABORT)

                (instance? onyx.types.BarrierAck message)
                ControlledFragmentHandler$Action/CONTINUE

                :else 
                (throw (ex-info "No other types of message exist."))))))

(defn controlled-fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn rotate [xs]
  (if (seq xs)
    (conj (into [] (rest xs)) (first xs))
    xs))

(defn task-alive? [event]
  (first (alts!! [(:onyx.core/kill-ch event) (:onyx.core/task-kill-ch event)] :default true)))

(defmethod extensions/receive-messages AeronMessenger
  [messenger {:keys [onyx.core/task-map onyx.core/id onyx.core/task-id onyx.core/task 
                     onyx.core/subscription-maps]
              :as event}]
  (let [rotated-subscriptions (swap! subscription-maps rotate)
        next-subscription (first (filter (comp nil? deref :barrier) rotated-subscriptions))]
    (if next-subscription
      (let [{:keys [subscription src-peer-id counter ticket-counter barrier]} next-subscription
            results (atom [])
            fh (controlled-fragment-data-handler
                 (fn [buffer offset length header]
                   (handle-message barrier results counter ticket-counter id task-id src-peer-id buffer offset length header)))]
        (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
        @results)
      [])))

(defn new-publication [peer-site]
  (info "Creating new pub")
  (let [channel (mc/aeron-channel (:aeron/external-addr peer-site) (:aeron/port peer-site))
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          (taoensso.timbre/warn "Aeron messaging publication error:" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        pub (.addPublication conn channel stream-id)]
    {:conn conn :pub pub}))

(defn write [^Publication pub ^UnsafeBuffer buf]
  ;; Needs an escape mechanism so it can break if a peer is shutdown
  ;; Needs an idle mechanism to prevent cpu burn
  (while (let [ret (.offer pub buf 0 (.capacity buf))] 
           (when (= ret Publication/CLOSED)
             (throw (Exception. "Wrote to closed publication.")))
           (neg? ret))
    (info "Re-offering message, session-id" (.sessionId pub))))

(defmethod extensions/send-messages AeronMessenger
  [messenger publication batch]
  (doseq [b batch]
    (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress b))]
      (write publication buf))))

(defmethod extensions/send-barrier AeronMessenger
  [messenger publication barrier]
  (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress barrier))]
    (write publication buf)))

(defmethod extensions/ack-barrier AeronMessenger
  [messenger publication ack-message]
  (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress ack-message))]
    (write publication buf)))
