(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message ->Barrier ->BarrierAck]]
            [onyx.messaging.messenger :as m]
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

(defrecord AeronMessagingPeerGroup [peer-config])

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessagingPeerGroup {:peer-config peer-config}))

(defn barrier? [v]
  (instance? onyx.types.Barrier v))

(defn message? [v]
  (instance? onyx.types.Message v))

(defn ack? [v]
  (instance? onyx.types.BarrierAck v))

(defn update-first-subscriber [messenger f]
  (update-in messenger [:subscriptions] f))

(defn set-ticket [messenger {:keys [src-peer-id dst-task-id slot-id]} ticket]
  (assoc-in messenger [:tickets src-peer-id dst-task-id slot-id] ticket))

(defn write [messenger {:keys [src-peer-id dst-task-id slot-id] :as task-slot} message]
  (throw (Exception.))
  ; (update-in messenger 
  ;            [:message-state src-peer-id dst-task-id slot-id]
  ;            (fn [messages] 
  ;              (conj (vec messages) message)))
  )

(defn rotate [xs]
  (if (seq xs)
    (conj (into [] (rest xs)) (first xs))
    xs))

(defn is-next-barrier? [messenger barrier]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn found-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  ;(info "barrier" (into {} barrier) "vs " (m/replica-version messenger))
  (and (is-next-barrier? messenger barrier) 
       (not (:emitted? barrier))))

(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (m/epoch messenger) (:epoch barrier))
       (:emitted? (:barrier subscriber))))

(defn get-message [messenger {:keys [src-peer-id dst-task-id slot-id] :as subscriber} ticket]
  (get-in messenger [:message-state src-peer-id dst-task-id slot-id ticket]))

(defn get-messages [messenger {:keys [src-peer-id dst-task-id slot-id] :as subscriber}]
  (get-in messenger [:message-state src-peer-id dst-task-id slot-id]))

(defn messenger->subscriptions [messenger]
  (get-in messenger [:subscriptions]))

(defn messenger->ack-subscriptions [messenger]
  (get-in messenger [:ack-subscriptions]))

(defn rotate-subscriptions [messenger]
  (update-in messenger [:subscriptions] rotate))

(defn curr-ticket [messenger {:keys [src-peer-id dst-task-id slot-id] :as subscriber}]
  (get-in messenger [:tickets src-peer-id dst-task-id slot-id]))

(defn next-barrier [messenger {:keys [src-peer-id dst-task-id slot-id position] :as subscriber} max-index]
  (let [missed-indexes (range (inc position) (inc max-index))] 
    (->> missed-indexes
         (map (fn [idx] 
                [idx (get-in messenger [:message-state src-peer-id dst-task-id slot-id idx])]))
         (filter (fn [[idx m]]
                   (and (barrier? m)
                        (is-next-barrier? messenger m))))
         first)))

(defn barrier->str [barrier]
  (str "B: " [(:replica-version barrier) (:epoch barrier)]))

(defn subscriber->str [subscriber]
  (str "S: " [(:src-peer-id subscriber) (:dst-task-id subscriber)]
       " STATE: " (barrier->str (:barrier subscriber))))

(defn take-messages [messenger subscriber]
  (let [ticket (curr-ticket messenger subscriber) 
        next-ticket (inc ticket)
        message (get-message messenger subscriber ticket)
        skip-to-barrier? (or (nil? (:barrier subscriber))
                             (and (< (inc (:position subscriber)) ticket) 
                                  (unblocked? messenger subscriber)))] 
    ;(println "Message is " message "Unblocked? " (unblocked? messenger subscriber))
    (cond skip-to-barrier?
          ;; Skip up to next barrier so we're aligned again, 
          ;; but don't move past actual messages that haven't been read
          (let [max-index (if (nil? (:barrier subscriber)) 
                            (dec (count (get-messages messenger subscriber)))
                            ticket)
                [position next-barrier] (next-barrier messenger subscriber max-index)]
            (debug (:id messenger) (subscriber->str subscriber) (barrier->str next-barrier))
            {:ticket (if position 
                       (max ticket (inc position))
                       ticket)
             :subscriber (if next-barrier 
                           (assoc subscriber :position position :barrier next-barrier)
                           subscriber)})

          ;; We're on the correct barrier so we can read messages
          (and message 
               (barrier? message) 
               (is-next-barrier? messenger message))
          ;; If a barrier, then update your subscriber's barrier to next barrier
          {:ticket next-ticket
           :subscriber (assoc subscriber :position ticket :barrier message)}

          ;; Skip over outdated barrier, and block subscriber until we hit the right barrier
          (and message 
               (barrier? message) 
               (> (m/replica-version messenger)
                  (:replica-version message)))
          {:ticket next-ticket
           :subscriber (assoc subscriber :position ticket :barrier nil)}

          (and message 
               (unblocked? messenger subscriber) 
               (message? message))
          ;; If it's a message, update the subscriber position
          {:message (:message message)
           :ticket next-ticket
           :subscriber (assoc subscriber :position ticket)}

          :else
          ;; Either nothing to read or we're past the current barrier, do nothing
          {:subscriber subscriber})))

(defn take-ack [messenger {:keys [src-peer-id dst-task-id slot-id position] :as subscriber}]
  (let [messages (get-in messenger [:message-state src-peer-id dst-task-id slot-id])
        _ (info "Trying to take ack from " 
                   src-peer-id dst-task-id
                   position
                   (vec messages))
        [idx ack] (->> messages
                       (map (fn [idx msg] [idx msg]) (range))
                       (drop (inc position))
                       (filter (fn [[idx ack]]
                                 (= (:replica-version ack) (m/replica-version messenger))))
                       (first))]
    (if ack 
      (assoc subscriber :barrier-ack ack :position idx)
      subscriber)))

(defn set-barrier-emitted [subscriber]
  (assoc-in subscriber [:barrier :emitted?] true))

(defn reset-messenger [messenger id]
  (-> messenger 
      (assoc-in [:subscriptions] [])
      (assoc-in [:ack-subscriptions] [])
      (assoc-in [:publications] [])))

(defn add-to-subscriptions [subscriptions sub-info]
  (conj (or subscriptions []) 
        (-> sub-info 
            (select-keys [:src-peer-id :dst-task-id :slot-id])
            (assoc :position -1))))

(defn remove-from-subscriptions [subscriptions sub-info]
  (filterv (fn [s] 
             (not= (select-keys sub-info [:src-peer-id :dst-task-id :slot-id]) 
                   (select-keys s [:src-peer-id :dst-task-id :slot-id])))
           subscriptions))

;; TICKETS SHOULD USE session id (unique publication) and position
;; Lookup task, then session id, then position, skip over positions that are lower, use ticket to take higher


;; Stick tickets in peer messenger group in single atom?
;; Have tickets be cleared up when image is no longer available?
;; Use these to manage tickets
;; onAvailableImage
;; onUnavailableImage

(defrecord AeronMessenger
  [peer-group id replica-version epoch message-state publications subscriptions ack-subscriptions]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    component)

  m/Messenger

  (publications [messenger]
    publications)

  (subscriptions [messenger]
    subscriptions)

  (ack-subscriptions [messenger]
    ack-subscriptions)

  (add-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info) (:slot-id sub-info)] 
                   #(or % 0))
        (update :subscriptions add-to-subscriptions sub-info)))

  (add-ack-subscription
    [messenger sub-info]
    (-> messenger 
        (update-in [:tickets (:src-peer-id sub-info) (:dst-task-id sub-info) (:slot-id sub-info)] 
                   #(or % 0))
        (update :ack-subscriptions add-to-subscriptions sub-info)))

  (remove-subscription
    [messenger sub-info]
    (-> messenger 
        (update :subscriptions remove-from-subscriptions sub-info)))

  (remove-ack-subscription
    [messenger sub-info]
    (-> messenger 
        (update :ack-subscriptions remove-from-subscriptions sub-info)))

  (add-publication
    [messenger pub-info]
    (update messenger
            :publications
            (fn [pbs] 
              (assert (= id (:src-peer-id pub-info)) [id (:src-peer-id pub-info)] )
              (conj (or pbs []) 
                    (select-keys pub-info [:src-peer-id :dst-task-id :slot-id])))))

  (remove-publication
    [messenger pub-info]
    (update messenger
               :publications
               (fn [pp] 
                 (filterv (fn [p] 
                            (not= (select-keys pub-info [:src-peer-id :dst-task-id :slot-id]) 
                                  (select-keys p [:src-peer-id :dst-task-id :slot-id])))
                          pp))))

  (set-replica-version [messenger replica-version]
    (-> messenger 
        (assoc :replica-version replica-version)
        (update :subscriptions (fn [ss] (mapv #(assoc % :barrier-ack nil :barrier nil) ss)))
        (m/set-epoch 0)))

  (replica-version [messenger]
    (get messenger :replica-version))

  (epoch [messenger]
    (println "EPOCH IS " epoch)
    epoch)

  (set-epoch 
    [messenger epoch]
    (assoc messenger :epoch epoch))

  (next-epoch
    [messenger]
    (update messenger :epoch inc))

  (receive-acks [messenger]
    (update messenger 
            :ack-subscriptions
            (fn [ss]
              (mapv (fn [s] (take-ack messenger s)) ss))))

  (flush-acks [messenger]
    (update messenger 
            :ack-subscriptions
            (fn [ss]
              (mapv #(assoc % :barrier-ack nil) ss))))

  (all-acks-seen? 
    [messenger]
    (if (empty? (remove :barrier-ack (messenger->ack-subscriptions messenger)))
      (select-keys (:barrier-ack (first (messenger->ack-subscriptions messenger))) 
                   [:replica-version :epoch])))

  (poll [messenger]
    (let [subscriber (first (messenger->subscriptions messenger))
          {:keys [message ticket] :as result} (take-messages messenger subscriber)] 
      ;(println (:id messenger) "TRying to take from " subscriber "message is " message)
      (debug (:id messenger) "MSG:" 
             (if message message "nil") 
             "New sub:" (subscriber->str (:subscriber result)))
      (cond-> (assoc messenger :message nil)
        ticket (set-ticket subscriber ticket)
        true (update-first-subscriber (constantly (:subscriber result)))
        true (rotate-subscriptions)
        message (assoc :message (t/input message)))))

  (offer-segments
    [messenger batch task-slots]
    (reduce (fn [m msg] 
              (reduce (fn [m* task-slot] 
                        (write m* task-slot (->Message id 
                                                       (:dst-task-id task-slot) 
                                                       (:slot-id task-slot)
                                                       msg)))
                      m
                      task-slots)) 
            messenger
            batch))

  (poll-recover [messenger]
    ;; Waits for the initial barriers when not 
    
    ;; read until got all barriers
    ;; Check they all have the same restore information
    ;; Return one restore information
    
    ;; Do loop of receive, all seen, emit, return {:epoch :replica-version}

    ;; FIXME hard coded "batch size"
    ; (println "Messages " 
    ;          (m/replica-version messenger)
    ;          (m/epoch messenger)
    ;          (m/all-barriers-seen? messenger)
    ;          (:message messenger))
      (if (m/all-barriers-seen? messenger)
        (let [recover (:recover (:barrier (first (messenger->subscriptions messenger))))] 
          (assert recover)
          (assoc messenger :recover recover))
        (assoc (m/poll messenger) :recover nil)))

  (emit-barrier [messenger]
    (onyx.messaging.messenger/emit-barrier messenger {}))

  (emit-barrier
    [messenger barrier-opts]
    (as-> messenger mn
      (m/next-epoch mn)
      (reduce (fn [m p] 
                (info "Emitting barrier " id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                (write m p (merge (->Barrier id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                                  barrier-opts))) 
              mn
              (get publications id))
      (update mn
              :subscriptions
              (fn [ss] 
                (mapv set-barrier-emitted ss)))))

  (all-barriers-seen? 
    [messenger]
    ; (println "Barriers seen:" 
    ;       (empty? (remove #(found-next-barrier? messenger %) 
    ;                       (messenger->subscriptions messenger)))
    ;       (vec (remove #(found-next-barrier? messenger %) 
    ;                                     (messenger->subscriptions messenger))))
    (empty? (remove #(found-next-barrier? messenger %) 
                    (messenger->subscriptions messenger))))

  (emit-barrier-ack
    [messenger]
    (as-> messenger mn 
      (reduce (fn [m p] 
                ;(info "Acking barrier to " id (:dst-task-id p) (m/replica-version mn) (m/epoch mn))
                (write m p (->BarrierAck id (:dst-task-id p) (m/replica-version mn) (m/epoch mn)))) 
              mn 
              (get publications id))
      (m/next-epoch mn)
      (update mn
              :subscriptions
              (fn [ss]
                (mapv set-barrier-emitted ss))))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (map->AeronMessenger {:id id :peer-config peer-config :messenger-group messenger-group}))

; (def fragment-limit-receiver 10)
; (def global-fragment-limit 10)

; (def no-op-error-handler
;   (reify ErrorHandler
;     (onError [this x] (taoensso.timbre/warn x))))

; (defn backoff-strategy [strategy]
;   (case strategy
;     :busy-spin (BusySpinIdleStrategy.)
;     :low-restart-latency (BackoffIdleStrategy. 100
;                                                10
;                                                (.toNanos TimeUnit/MICROSECONDS 1)
;                                                (.toNanos TimeUnit/MICROSECONDS 100))
;     :high-restart-latency (BackoffIdleStrategy. 1000
;                                                 100
;                                                 (.toNanos TimeUnit/MICROSECONDS 10)
;                                                 (.toNanos TimeUnit/MICROSECONDS 1000))))

; (defrecord AeronMessenger
;   [peer-group messenger-group publication-group publications
;    send-idle-strategy compress-f monitoring short-ids acking-ch]
;   component/Lifecycle

;   (start [component]
;     (taoensso.timbre/info "Starting Aeron Messenger")
;     (let [config (:config peer-group)
;           messenger-group (:messenger-group peer-group)
;           publications (atom {})
;           send-idle-strategy (:send-idle-strategy messenger-group)
;           compress-f (:compress-f messenger-group)
;           short-ids (atom {})]
;       (assoc component
;              :messenger-group messenger-group
;              :short-ids short-ids
;              :send-idle-strategy send-idle-strategy
;              :publications publications
;              :compress-f compress-f)))

;   (stop [{:keys [short-ids publications] :as component}]
;     (taoensso.timbre/info "Stopping Aeron Messenger")
;     (run! (fn [{:keys [pub conn]}] 
;             (.close pub)
;             (.close conn)) 
;           (vals @publications))

;     (assoc component
;            :messenger-group nil
;            :send-idle-strategy nil
;            :publications nil
;            :short-ids nil
;            :compress-f nil)))

; #_(defmethod extensions/register-task-peer AeronMessenger
;   [{:keys [short-ids] :as messenger}
;    {:keys [aeron/peer-task-id]}
;    task-buffer]
;   #_(swap! short-ids assoc :peer-task-short-id peer-task-id))

; #_(defmethod extensions/unregister-task-peer AeronMessenger
;   [{:keys [short-ids] :as messenger}
;    {:keys [aeron/peer-task-id]}]
;   #_(swap! short-ids dissoc peer-task-id))

(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

; (defrecord AeronPeerGroup [opts subscribers ticketing-counters compress-f decompress-f send-idle-strategy]
;   component/Lifecycle
;   (start [component]
;     (taoensso.timbre/info "Starting Aeron Peer Group")
;     (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? opts)
;           threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading opts))

;           media-driver-context (if embedded-driver?
;                                  (-> (MediaDriver$Context.) 
;                                      (.threadingMode threading-mode)
;                                      (.dirsDeleteOnStart true)))

;           media-driver (if embedded-driver?
;                          (MediaDriver/launch media-driver-context))

;           bind-addr (common/bind-addr opts)
;           external-addr (common/external-addr opts)
;           port (:onyx.messaging/peer-port opts)
;           poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts)
;           offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy opts)
;           send-idle-strategy (backoff-strategy poll-idle-strategy-config)
;           receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
;           compress-f (or (:onyx.messaging/compress-fn opts) messaging-compress)
;           decompress-f (or (:onyx.messaging/decompress-fn opts) messaging-decompress)
;           ticketing-counters (atom {})
;           ctx (.errorHandler (Aeron$Context.) no-op-error-handler)]
;       (when embedded-driver? 
;         (.addShutdownHook (Runtime/getRuntime) 
;                           (Thread. (fn [] 
;                                      (.deleteAeronDirectory ^MediaDriver$Context media-driver-context)))))
;       (assoc component
;              :bind-addr bind-addr
;              :external-addr external-addr
;              :media-driver-context media-driver-context
;              :media-driver media-driver
;              :compress-f compress-f
;              :decompress-f decompress-f
;              :ticketing-counters ticketing-counters
;              :port port
;              :send-idle-strategy send-idle-strategy)))

;   (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
;     (taoensso.timbre/info "Stopping Aeron Peer Group")

;     (when media-driver (.close ^MediaDriver media-driver))
;     (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
;     (assoc component
;            :bind-addr nil :external-addr nil :media-driver nil :media-driver-context nil 
;            :external-channel nil :compress-f nil :decompress-f nil :ticketing-counters nil 
;            :send-idle-strategy nil)))

; (defmethod clojure.core/print-method AeronPeerGroup
;   [system ^java.io.Writer writer]
;   (.write writer "#<Aeron Peer Group>"))

; (defn aeron-peer-group [opts]
;   (map->AeronPeerGroup {:opts opts}))

; (def possible-ids
;   (set (map short (range -32768 32768))))

; (defn available-ids [used]
;   (clojure.set/difference possible-ids used))

; (defn choose-id [hsh used]
;   (when-let [available (available-ids used)]
;     (nth (seq available) (mod hsh (count available)))))

; (defn allocate-id [peer-id peer-site peer-sites]
;   ;;; Assigns a unique id to each peer so that messages do not need
;   ;;; to send the entire peer-id in a payload, saving 14 bytes per
;   ;;; message
;   (let [used-ids (->> (vals peer-sites)
;                       (filter
;                         (fn [s]
;                           (= (:aeron/external-addr peer-site)
;                              (:aeron/external-addr s))))
;                       (map :aeron/peer-id)
;                       set)
;         id (choose-id peer-id used-ids)]
;     (when-not id
;       (throw (ex-info "Couldn't assign id. Ran out of aeron ids. 
;                       This should only happen if more than 65356 virtual peers have been started up on a single external addr."
;                       peer-site)))
;     id))

; (defmethod m/assign-task-resources :aeron
;   [replica peer-id task-id peer-site peer-sites]
;   {}
;   #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

; (defmethod m/get-peer-site :aeron
;   [replica peer]
;   (get-in replica [:peer-sites peer :aeron/external-addr]))

; (defn aeron-messenger [peer-config messenger-group]
;   (map->AeronMessenger {:peer-config peer-config :messenger-group messenger-group}))

; #_(defmethod m/peer-site AeronMessenger
;   [messenger]
;   {:aeron/external-addr (:external-addr (:messenger-group messenger))
;    :aeron/port (:port (:messenger-group messenger))})

; (defrecord AeronPeerConnection [channel stream-id peer-task-id])

; ;; Define stream-id as only allowed stream
; (def stream-id 1)

; ; (defmethod m/connection-spec AeronMessenger
; ;   [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/peer-task-id] :as peer-site}]
; ;   (->AeronPeerConnection (mc/aeron-channel external-addr port) stream-id peer-task-id))

; #_(defmethod m/shared-ticketing-counter AeronMessenger
;   [messenger job-id peer-id task-id]
;   (let [path [job-id task-id peer-id]] 
;     (get-in (swap! (:ticketing-counters (:messenger-group messenger)) 
;                    (fn [tc]
;                      (if (get-in tc path)
;                         tc
;                        (assoc-in tc path (atom 0)))))
;             path)))

; #_(defmethod m/new-partial-subscriber AeronMessenger
;   [{:keys [messenger-group] :as messenger} job-id peer-id task-id]
;   (info "new subscriber for " job-id peer-id task-id)
;   (let [error-handler (reify ErrorHandler
;                         (onError [this x] 
;                           (taoensso.timbre/warn "Aeron messaging subscriber error:" x)))
;         ctx (-> (Aeron$Context.)
;                 (.errorHandler error-handler))
;         conn (Aeron/connect ctx)
;         bind-addr (:bind-addr messenger-group)
;         port (:port messenger-group)
;         channel (mc/aeron-channel bind-addr port)
;         subscription (.addSubscription conn channel stream-id)]
;     {:subscription subscription
;      :conn conn
;      :counter (atom 0)
;      :ticket-counter (m/shared-ticketing-counter messenger job-id peer-id task-id)
;      :barrier (atom nil)
;      :src-peer-id peer-id}))

; #_(defmethod m/close-partial-subscriber AeronMessenger
;   [{:keys [messenger-group] :as messenger} partial-subscriber]
;   (info "Closing partial subscriber")
;   (.close ^Subscription (:subscription partial-subscriber))
;   (.close ^Aeron (:conn partial-subscriber)))

; (defn handle-message
;   [barrier results subscriber-counter ticket-counter this-peer-id this-task-id src-peer-id buffer offset length header]
;   (let [ba (byte-array length)
;         _ (.getBytes buffer offset ba)
;         message (messaging-decompress ba)
;         n-desired-messages 2]
;     ;(info "handling message " (into {} message))
;     (cond (>= (count @results) n-desired-messages)
;           ControlledFragmentHandler$Action/ABORT
;           (and (= (:dst-task-id message) this-task-id)
;                (= (:src-peer-id message) src-peer-id))
;           (cond (instance? onyx.types.Leaf message)
;                 (let [message-id @subscriber-counter 
;                       ticket-id @ticket-counter]
;                   (swap! subscriber-counter inc)
;                   (cond (< message-id ticket-id)
;                         ControlledFragmentHandler$Action/CONTINUE
;                         (= message-id ticket-id)
;                         (do (when (compare-and-set! ticket-counter ticket-id (inc ticket-id))
;                               (swap! results conj message))
;                             ControlledFragmentHandler$Action/CONTINUE)
;                         (> message-id ticket-id)
;                         (throw (ex-info "Shouldn't be possible to get ahead of a ticket id " {:message-id message-id :ticket-id ticket-id}))))
;                 (instance? onyx.types.Barrier message)
;                 (if (empty? @results)
;                   (do (reset! barrier message)
;                       ControlledFragmentHandler$Action/BREAK)  
;                   ControlledFragmentHandler$Action/ABORT)

;                 (instance? onyx.types.BarrierAck message)
;                 ControlledFragmentHandler$Action/CONTINUE

;                 :else 
;                 (throw (ex-info "No other types of message exist."))))))

; (defn controlled-fragment-data-handler [f]
;   (ControlledFragmentAssembler.
;     (reify ControlledFragmentHandler
;       (onFragment [this buffer offset length header]
;         (f buffer offset length header)))))

; (defn rotate [xs]
;   (if (seq xs)
;     (conj (into [] (rest xs)) (first xs))
;     xs))

; (defn task-alive? [event]
;   (first (alts!! [(:kill-ch event) (:task-kill-ch event)] :default true)))

; #_(defmethod m/receive-messages AeronMessenger
;   [messenger {:keys [task-map id task-id task 
;                                 subscription-maps]
;                          :as event}]
;   (let [rotated-subscriptions (swap! subscription-maps rotate)
;         next-subscription (first (filter (comp nil? deref :barrier) rotated-subscriptions))]
;     (if next-subscription
;       (let [{:keys [subscription src-peer-id counter ticket-counter barrier]} next-subscription
;             results (atom [])
;             fh (controlled-fragment-data-handler
;                  (fn [buffer offset length header]
;                    (handle-message barrier results counter ticket-counter id task-id src-peer-id buffer offset length header)))]
;         (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
;         @results)
;       [])))

; (defn new-publication [peer-site]
;   (info "Creating new pub")
;   (let [channel (mc/aeron-channel (:aeron/external-addr peer-site) (:aeron/port peer-site))
;         error-handler (reify ErrorHandler
;                         (onError [this x] 
;                           (taoensso.timbre/warn "Aeron messaging publication error:" x)))
;         ctx (-> (Aeron$Context.)
;                 (.errorHandler error-handler))
;         conn (Aeron/connect ctx)
;         pub (.addPublication conn channel stream-id)]
;     {:conn conn :pub pub}))

; (defn write [^Publication pub ^UnsafeBuffer buf]
;   ;; Needs an escape mechanism so it can break if a peer is shutdown
;   ;; Needs an idle mechanism to prevent cpu burn
;   (while (let [ret (.offer pub buf 0 (.capacity buf))] 
;            (when (= ret Publication/CLOSED)
;              (throw (Exception. "Wrote to closed publication.")))
;            (neg? ret))
;     (info "Re-offering message, session-id" (.sessionId pub))))

; #_(defmethod m/offer-segments AeronMessenger
;   [messenger publication batch]
;   (doseq [b batch]
;     (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress b))]
;       (write publication buf))))

; #_(defmethod m/send-barrier AeronMessenger
;   [messenger publication barrier]
;   (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress barrier))]
;     (write publication buf)))

; #_(defmethod m/ack-barrier AeronMessenger
;   [messenger publication ack-message]
;   (let [buf ^UnsafeBuffer (UnsafeBuffer. (messaging-compress ack-message))]
;     (write publication buf)))
