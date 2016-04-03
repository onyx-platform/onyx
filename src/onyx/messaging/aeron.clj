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

(defn update-global-watermarks [gw dst-task-id src-peer res barrier?]
  (let [hw (inc (get-in gw [dst-task-id src-peer :high-water-mark] -1))
        gw* (assoc-in gw [dst-task-id src-peer :high-water-mark] hw)]
    (if barrier?
      (-> gw*
          (assoc-in [dst-task-id src-peer :barriers (:barrier-epoch res)] #{})
          (assoc-in [dst-task-id src-peer :barrier-index (:barrier-epoch res)] hw))
      gw*)))

(defn global-stream-observer-handler [global-watermarks buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes buffer offset ba)
        res (messaging-decompress ba)
        src-peer (:src-peer-id res)
        dst-task-id (:dst-task-id res)
        barrier? (instance? onyx.types.Barrier res)]
    (when-not (= :job-completed (:type res))
      (swap! global-watermarks update-global-watermarks dst-task-id src-peer res barrier?))))

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

(defn start-global-stream-observer! [conn bind-addr port stream-id idle-strategy global-watermarks]
  (let [channel (aeron-channel bind-addr port)
        subscription (.addSubscription conn channel stream-id)
        handler (global-fragment-data-handler
                 (fn [buffer offset length header]
                   (global-stream-observer-handler global-watermarks buffer offset length header)))
        subscription-fut (future (try (.accept ^Consumer (global-consumer handler idle-strategy global-fragment-limit) subscription)
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
          short-ids (atom {})]
      (assoc component
             :messaging-group messaging-group
             :short-ids short-ids
             :send-idle-strategy send-idle-strategy
             :publications publications
             :compress-f compress-f)))

  (stop [{:keys [short-ids publications] :as component}]
    (taoensso.timbre/info "Stopping Aeron Messenger")
    (run! #(.close %) (vals @publications))

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
          decompress-f (or (:onyx.messaging/decompress-fn opts) messaging-decompress)
          ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
          aeron-conn (Aeron/connect ctx)
          global-watermarks (atom {})
          global-stream-observer (start-global-stream-observer!
                                  aeron-conn
                                  bind-addr
                                  port
                                  1
                                  receive-idle-strategy
                                  global-watermarks)]
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
             :send-idle-strategy send-idle-strategy
             :aeron-conn aeron-conn
             :global-stream-observer global-stream-observer
             :global-watermarks global-watermarks)))

  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")

    (future-cancel (:subscription-fut (:global-stream-observer component)))
    (.close ^Subscription (:subscription (:global-stream-observer component)))
    (.close ^Aeron (:aeron-conn component))


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
  [result-state message-counter task-id src-peer-id [low high :as ticket] buffer offset length header]
  (let [this-msg-index (get @message-counter src-peer-id 0)]
    (let [ba (byte-array length)
          _ (.getBytes buffer offset ba)
          res (messaging-decompress ba)]
      (if (and (= (:src-peer-id res) src-peer-id)
               (= (:dst-task-id res) task-id))
        (cond (< this-msg-index low)
              (do (swap! message-counter assoc src-peer-id (inc this-msg-index))
                  ControlledFragmentHandler$Action/CONTINUE)

              (> this-msg-index high)
              ControlledFragmentHandler$Action/ABORT

              (instance? onyx.types.Barrier res)
              (do (swap! result-state conj (assoc res :msg-id this-msg-index))
                  (swap! message-counter assoc src-peer-id (inc this-msg-index))
                  ControlledFragmentHandler$Action/BREAK)

              (instance? onyx.types.Leaf res)
              (do (swap! result-state conj res)
                  (swap! message-counter assoc src-peer-id (inc this-msg-index))
                  ControlledFragmentHandler$Action/CONTINUE)

              :else (throw (ex-info "Failed to handle incoming Aeron message"
                                    {:low low
                                     :high high
                                     :this-task-id task-id
                                     :src-peer-id src-peer-id
                                     :this-msg-index this-msg-index
                                     :res res})))
        ControlledFragmentHandler$Action/CONTINUE))))

(defn controlled-fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn rotate [xs]
  (if (seq xs)
    (conj (into [] (rest xs)) (first xs))
    xs))

(defn remove-blocked-consumers
  "Implements barrier alignment"
  [replica job-id task-id global-watermarks subscription-maps]
  (remove
   (fn [{:keys [src-peer-id]}]
     (let [barrier-state (get-in global-watermarks [task-id src-peer-id :barriers])]
       (some #{src-peer-id} (get barrier-state (first (keys barrier-state))))))
   subscription-maps))

(defn unseen-barriers [barriers peer-id]
  (reduce-kv
   (fn [result barrier-epoch peers]
     (if-not (get peers peer-id)
       (assoc result barrier-epoch peers)
       result))
   {}
   barriers))

;; Terms:
;; Low water mark: last message index that's been processed
;; High water mark: latest known message by stream observer
;; Ticket: message range that a peer should read from a stream

;; Example -
;;
;; Last ticket: [0 2]
;; Stream:
;;
;; [  0  ] [  1  ] [  2  ] [  3  ] [  4  ] [   Barrier 1  ] [  5  ] [  6  ]
;;                    ^                                                ^
;;                    |                                                |
;;                    |                                                |
;;              low water mark                                   high water mark
;;

;; Next ticket calculation:
;; Low position = last ticket high position + 1
;; 
;; High position = Low + (number of messages to read - 1)
;; (subtract 1 because we're already incrementing by 1 when we move the low water mark)
;;
;; High position may need to be adjusted:
;;
;; 1. if there is a barrier we haven't processed in between Low and High,
;;    High becomes the barrier position.
;;
;; 2. if High is > high water mark, High becomes high water mark.

;; Ticket failure cases:
;; We will *not* send out a new ticket (return -1) if the following happens:
;; 1. Low water mark = High water mark
;; 2. High watermark does not exist. Stream observer needs to catch up.

(defn calculate-ticket [{:keys [ticket high-water-mark barriers barrier-index] :as watermarks} my-peer-id take-n]
  (let [low-water-mark (or (second ticket) -1)]
    (if (or (not high-water-mark) (= low-water-mark high-water-mark))
      -1
      (let [barriers* (unseen-barriers barriers my-peer-id)
            nearest-barrier (first (sort (keys barriers*)))
            nearest-barrier-pos (get barrier-index nearest-barrier high-water-mark)
            new-low (inc low-water-mark)
            new-high (+ new-low (dec take-n))
            _ (prn nearest-barrier)
            result
            (cond (> new-high high-water-mark)
                  [new-low high-water-mark]

                  (< nearest-barrier-pos new-high)
                  [new-low nearest-barrier-pos]

                  :else [new-low new-high])]
        (when nearest-barrier
          (assert (>= nearest-barrier-pos (first result))
                  {:msg "Next barrier is behind the lower ticket bound"
                   :ticket result
                   :barrier {:barrier nearest-barrier
                             :position nearest-barrier-pos}}))
        (assert (<= (first result) (second result))
                {:msg "Ticket bounds were out of order"
                 :expr (str (first result) " > " (second result))
                 :watermarks watermarks})
        result))))

(defn take-ticket [global-watermarks task-id src-peer-id peer-id take-n]
  (let [ticket (calculate-ticket (get-in global-watermarks [task-id src-peer-id]) peer-id take-n)]
    (if (= -1 ticket)
      (assoc-in global-watermarks [task-id src-peer-id :new-ticket?] false)
      (-> global-watermarks
          (assoc-in [task-id src-peer-id :new-ticket?] true)
          (assoc-in [task-id src-peer-id :ticket] ticket)))))

(defn task-alive? [event]
  (first (alts!! [(:onyx.core/kill-ch event) (:onyx.core/task-kill-ch event)] :default true)))

(defmethod extensions/receive-messages AeronMessenger
  [messenger {:keys [onyx.core/subscriptions onyx.core/task-map onyx.core/replica onyx.core/job-id onyx.core/id 
                     onyx.core/task-id onyx.core/task onyx.core/message-counter onyx.core/global-watermarks
                     onyx.core/messenger-buffer onyx.core/subscription-maps]
              :as event}]
  (let [rotated-subscriptions (swap! subscription-maps rotate)
        removed-subscriptions (remove-blocked-consumers @replica job-id task-id @global-watermarks rotated-subscriptions)
        next-subscription (first removed-subscriptions)]
    (if next-subscription
      (let [{:keys [subscription src-peer-id]} next-subscription
            result-state (atom [])
            take-n 2
            gw-val (swap! global-watermarks take-ticket task-id src-peer-id id take-n)]
        (if (get-in gw-val [task-id src-peer-id :new-ticket?])
          (let [ticket (get-in gw-val [task-id src-peer-id :ticket])
                fh (controlled-fragment-data-handler (partial handle-message result-state message-counter task-id src-peer-id ticket))
                expected-messages (inc (- (second ticket) (first ticket)))]
            (while (and (< (count @result-state) expected-messages) 
                        (task-alive? event))
              (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver))
            @result-state)
          []))
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
    (debug "Re-offering message, session-id" (.sessionId pub))))

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
