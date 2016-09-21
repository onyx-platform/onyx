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

(def fragment-limit-receiver 10)
(def global-fragment-limit 10)

(def no-op-error-handler
  (reify ErrorHandler
    ;; FIXME, this should probably cause restarting peers/ peer groups etc
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


(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defn stream-id [job-id task-id slot-id]
  (hash [job-id task-id slot-id]))

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  (println "GET PEER SITE" (mc/external-addr peer-config))
  {:address (mc/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

(defrecord AeronMessagingPeerGroup [peer-config]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? peer-config)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading peer-config))
          media-driver-context (if embedded-driver?
                                 (-> (MediaDriver$Context.) 
                                     (.threadingMode threading-mode)
                                     (.dirsDeleteOnStart true)))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))
          bind-addr (common/bind-addr peer-config)
          external-addr (common/external-addr peer-config)
          port (:onyx.messaging/peer-port peer-config)
          poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy peer-config)
          offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy peer-config)
          send-idle-strategy (backoff-strategy poll-idle-strategy-config)
          receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn peer-config) messaging-compress)
          decompress-f (or (:onyx.messaging/decompress-fn peer-config) messaging-decompress)
          ticket-counters (atom {})
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
             :ticket-counters ticket-counters
             :port port
             :send-idle-strategy send-idle-strategy)))

  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component
           :bind-addr nil :external-addr nil :media-driver nil :media-driver-context nil 
           :external-channel nil :compress-f nil :decompress-f nil :ticket-counters nil 
           :send-idle-strategy nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessagingPeerGroup {:peer-config peer-config}))

(defn barrier? [v]
  (instance? onyx.types.Barrier v))

(defn message? [v]
  (instance? onyx.types.Message v))

(defn ack? [v]
  (instance? onyx.types.BarrierAck v))

(defn subscription-ticket 
  [{:keys [replica-version ticket-counters] :as messenger} 
   {:keys [dst-task-id src-peer-id] :as sub}]
  (get-in @ticket-counters [replica-version [src-peer-id dst-task-id]]))

(defn subscription-aligned?
  [sub-ticket]
  (empty? (:aligned sub-ticket)))

;; FIXME, need a way to bail out of sending out the initial barriers
;; as peers may no longer exist
;; Should check some replica atom here to see if anything has changed
;; Or maybe tasks need some flag that says stuff is out of date or task is dead
(defn offer-until-success! [messenger task-slot message]
  (let [publication ^Publication (:publication task-slot)
        buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress message))]
    (while (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
             (when (neg? ret) (println "OFFERED, GOT" ret Publication/CLOSED))
             (when (= ret Publication/CLOSED)
               (throw (Exception. "Wrote to closed publication.")))
             (neg? ret))
      (println "Re-offering message, session-id" (.sessionId publication))))
  messenger)

(defn is-next-barrier? [messenger barrier]
  (assert (m/replica-version messenger))
  (and (= (m/replica-version messenger) (:replica-version barrier))
       (= (inc (m/epoch messenger)) (:epoch barrier))))

(defn found-next-barrier? [messenger {:keys [barrier] :as subscriber}]
  (let [barrier-val @barrier] 
    (and (is-next-barrier? messenger barrier-val) 
         (not (:emitted? barrier-val)))))

(defn unblocked? [messenger {:keys [barrier] :as subscriber}]
  (info "Unblocked?" subscriber (let [barrier-val @barrier] 
    (and (= (m/replica-version messenger) (:replica-version barrier-val))
         (= (m/epoch messenger) (:epoch barrier-val))
         (:emitted? barrier-val))))
  (let [barrier-val @barrier] 
    (and (= (m/replica-version messenger) (:replica-version barrier-val))
         (= (m/epoch messenger) (:epoch barrier-val))
         (:emitted? barrier-val))))

(defn handle-read-segments
  [messenger results barrier ticket dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)
        ;; FIXME, why 2?
        n-desired-messages 2
        ticket-val @ticket
        position (.position header)]
    #_(info "handling message " (type message) (into {} message)
            "reading task" dst-task-id "peer" src-peer-id "our epoch" (m/epoch messenger) 
            "our replica" (m/replica-version messenger) (is-next-barrier? messenger message))
    (cond (>= (count results) n-desired-messages)
          ControlledFragmentHandler$Action/ABORT
          (and (= (:dst-task-id message) dst-task-id)
               (= (:src-peer-id message) src-peer-id))
          (cond (and (message? message)
                     (not (nil? @barrier))
                     (< ticket-val position))
                (do 
                 (assert (= (m/replica-version messenger) (:replica-version message)))
                 ;; FIXME, not sure if this logically works.
                 ;; If ticket gets updated in mean time, then is this always invalid and should be continued?
                 ;; WORK OUT ON PAPER
                 (when (compare-and-set! ticket ticket-val position)
                   (do 
                    (assert (coll? (:payload message)))
                    (reduce conj! results (:payload message))))
                 ControlledFragmentHandler$Action/CONTINUE)

                (and (barrier? message)
                     (> (m/replica-version messenger)
                        (:replica-version message)))
                (do
                 (info "Dropping message 1" (into {} message))
                 ControlledFragmentHandler$Action/CONTINUE)

                (and (barrier? message)
                     (< (m/replica-version messenger)
                        (:replica-version message)))
                ControlledFragmentHandler$Action/ABORT

                (and (barrier? message)
                     (is-next-barrier? messenger message))
                (if (zero? (count results)) ;; empty? broken on transients
                  (do 
                   (reset! barrier message)
                   ControlledFragmentHandler$Action/BREAK)  
                  ControlledFragmentHandler$Action/ABORT)

                (and (barrier? message)
                     (= (m/replica-version messenger)
                        (:replica-version message)))
                (throw (Exception. "Should not happen"))

                ;; This can happen when ticketing and we're ignoring the message
                :else 
                (do
                 (info "Dropping message 2" (into {} message) ticket-val (.position header) (not (nil? @barrier)) (m/replica-version messenger))
                 ControlledFragmentHandler$Action/CONTINUE))

          (= (:replica-version message) (m/replica-version messenger))
          (do
           (info "Dropping message 3" (into {} message))
           ControlledFragmentHandler$Action/CONTINUE)
          
          :else
          ControlledFragmentHandler$Action/ABORT)))

;; TODO, do not re-ify on every read
(defn controlled-fragment-data-handler [f]
  (ControlledFragmentAssembler.
    (reify ControlledFragmentHandler
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn handle-poll-new-barrier
  [messenger barrier dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    (info "POLL NEW BARRIER" (into {} message))
    (cond (and (= (:dst-task-id message) dst-task-id)
               (= (:src-peer-id message) src-peer-id)
               (barrier? message)
               ;(nil? @barrier)
               (is-next-barrier? messenger message))
          (do 
          ; (info "GOT NEW BARRIER ON RECOVER" (:id messenger) (into {} message) dst-task-id)
           (reset! barrier message)
           ControlledFragmentHandler$Action/BREAK)

          (or (not= (:dst-task-id message) dst-task-id)
              (not= (:src-peer-id message) src-peer-id)
              (< (:replica-version message) (m/replica-version messenger)))
          ControlledFragmentHandler$Action/CONTINUE

          :else
          ControlledFragmentHandler$Action/ABORT)))

(defn poll-messages! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier]} sub-info
        sub-ticket (subscription-ticket messenger sub-info)]
    ;; May not need to check for alignment here, can prob just do in :recover
    (info "POLL" (subscription-aligned? sub-ticket)
          (unblocked? messenger sub-info)
          sub-info)
    (if (subscription-aligned? sub-ticket)
      (if (unblocked? messenger sub-info)
        ;; Poll for new messages and barriers
        (let [results (transient [])
              ;; FIXME, maybe shouldn't reify a controlled fragment handler each time?
              ;; Put the fragment handler in the sub info?
              fh (controlled-fragment-data-handler
                  (fn [buffer offset length header]
                    (handle-read-segments messenger results barrier (:ticket sub-ticket) dst-task-id src-peer-id buffer offset length header)))]
          (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
          (persistent! results))
        ;; Try to find next barrier
        ;(throw (Exception. "Shouldn't happen?"))
        ;; FIXME: split out
        #_(let [fh (controlled-fragment-data-handler
                  (fn [buffer offset length header]
                    (handle-poll-new-barrier messenger barrier dst-task-id src-peer-id buffer offset length header)))]
          (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
          [])
        []
        
        )
      [])))

(defn poll-new-replica! [messenger sub-info]
  (let [{:keys [src-peer-id dst-task-id subscription barrier]} sub-info
        sub-ticket (subscription-ticket messenger sub-info)]
    ;; May not need to check for alignment here, can prob just do in :recover
    (if (subscription-aligned? sub-ticket)
      (let [fh (controlled-fragment-data-handler
                (fn [buffer offset length header]
                  (handle-poll-new-barrier messenger barrier dst-task-id src-peer-id buffer offset length header)))]
        (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver))
      (info "SUB NOT ALIGNED"))))

;; TODO, can possibly take more than one ack at a time from a sub?
(defn handle-poll-acks [messenger barrier-ack dst-task-id src-peer-id buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    (if (and (= (:dst-task-id message) dst-task-id)
             (= (:src-peer-id message) src-peer-id)
             (ack? message)
             (= (m/replica-version messenger) (:replica-version message)))
      (do 
       (info "GOT NEW BARRIER ACK" (into {} message))
       (reset! barrier-ack message)
       ControlledFragmentHandler$Action/BREAK)
      ControlledFragmentHandler$Action/CONTINUE)))

(defn poll-acks! [messenger sub-info]
  (if @(:barrier-ack sub-info)
    messenger
    (let [{:keys [src-peer-id dst-task-id subscription barrier-ack ticket-counter]} sub-info
          fh (controlled-fragment-data-handler
              (fn [buffer offset length header]
                (handle-poll-acks messenger barrier-ack dst-task-id src-peer-id buffer offset length header)))]
      (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler fh fragment-limit-receiver)
      messenger)))

(defn new-subscription 
  [{:keys [messenger-group id] :as messenger}
   {:keys [job-id src-peer-id dst-task-id slot-id] :as sub-info}]
  (info "new subscriber for " job-id src-peer-id dst-task-id)
  (let [error-handler (reify ErrorHandler
                        (onError [this x] 
                          ;; FIXME: Reboot peer
                          (taoensso.timbre/warn x "Aeron messaging subscriber error")))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        ;; Maybe use site from sub-info instead?
        bind-addr (:bind-addr messenger-group)
        port (:port messenger-group)
        channel (mc/aeron-channel bind-addr port)
        stream (stream-id job-id dst-task-id slot-id)
        _ (println "Add subscription " channel stream)
        subscription (.addSubscription conn channel stream)]
    (assoc sub-info
           :subscription subscription
           :conn conn
           :barrier-ack (atom nil)
           :barrier (atom nil))))

(defn new-publication [{:keys [messenger-group] :as messenger}
                       {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (let [channel (mc/aeron-channel (:address site) (:port site))
        error-handler (reify ErrorHandler
                        (onError [this x] 
                          ;; FIXME: Reboot peer
                          (taoensso.timbre/warn "Aeron messaging publication error:" x)))
        ctx (-> (Aeron$Context.)
                (.errorHandler error-handler))
        conn (Aeron/connect ctx)
        stream (stream-id job-id dst-task-id slot-id)
        _ (println "Creating new pub" channel stream)
        pub (.addPublication conn channel stream)]
    (assoc pub-info :conn conn :publication pub)))

(defn add-to-subscriptions [subscriptions sub-info]
  (conj (or subscriptions []) sub-info))

(defn remove-from-subscriptions [subscriptions {:keys [dst-task-id slot-id] :as sub-info}]
  (update-in subscriptions
             [dst-task-id slot-id]
             (fn [ss] 
               (filterv (fn [s] 
                        (not= (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site]) 
                              (select-keys s [:src-peer-id :dst-task-id :slot-id :site])))
                      ss))))

(defn remove-from-publications [publications pub-info]
  (filterv (fn [p] 
             (not= (select-keys pub-info [:src-peer-id :dst-task-id :slot-id :site]) 
                   (select-keys p [:src-peer-id :dst-task-id :slot-id :site])))
           publications))

;; TICKETS SHOULD USE session id (unique publication) and position
;; Lookup task, then session id, then position, skip over positions that are lower, use ticket to take higher
;; Stick tickets in peer messenger group in single atom?
;; Have tickets be cleared up when image is no longer available?
;; Use these to manage tickets
;; onAvailableImage
;; onUnavailableImage

(defn flatten-publications [publications]
  (reduce (fn [all [dst-task-id ps]]
            (into all (mapcat (fn [[slot-id pubs]]
                                pubs)
                              ps)))
          []
          publications))

(defn set-barrier-emitted! [subscriber]
  (assert (not (:emitted? (:barrier subscriber))))
  (swap! (:barrier subscriber) assoc :emitted? true))

(defn allocation-changed? [replica job-id replica-version]
  (and (some #{job-id} (:jobs replica))
       (not= replica-version
             (get-in replica [:allocation-version job-id]))))

(defrecord AeronMessenger [messenger-group ticket-counters id replica-version epoch 
                           publications subscriptions ack-subscriptions read-index]
  component/Lifecycle
  (start [component]
    (assoc component
           :ticket-counters 
           (:ticket-counters messenger-group)))

  (stop [component]
    (run! (fn [pub]
            (.close ^Publication (:publication pub)))
          (flatten-publications publications))
    (run! (fn [sub]
            (.close ^Subscription (:subscription sub)))
          (concat subscriptions ack-subscriptions))
    (assoc component 
           :ticket-counters nil :replica-version nil 
           :epoch nil :publications nil :subscription nil 
           :ack-subscriptions nil :read-index nil :!replica nil))

  m/Messenger
  (publications [messenger]
    (flatten-publications publications))

  (subscriptions [messenger]
    subscriptions)

  (ack-subscriptions [messenger]
    ack-subscriptions)

  (add-subscription [messenger sub-info]
    (update messenger :subscriptions add-to-subscriptions (new-subscription messenger sub-info)))

  (register-ticket [messenger sub-info]
    (swap! ticket-counters 
           update 
           replica-version 
           (fn [tickets]
             (update (or tickets {}) 
                     [(:src-peer-id sub-info)
                      (:dst-task-id sub-info)]
                     (fn [sub-ticket]
                       (if sub-ticket
                         ;; Already know what peers should be aligned
                         (update sub-ticket :aligned disj id)
                         {:ticket (atom -1)
                          :aligned (disj (set (:aligned-peers sub-info)) id)})))))
    messenger)

  (add-ack-subscription [messenger sub-info]
    (update messenger :ack-subscriptions add-to-subscriptions (new-subscription messenger sub-info)))

  (remove-subscription [messenger sub-info]
    (.close ^Subscription (:subscription sub-info))
    (update messenger :subscriptions remove-from-subscriptions sub-info))

  (remove-ack-subscription [messenger sub-info]
    (.close ^Subscription (:subscription sub-info))
    (update messenger :ack-subscriptions remove-from-subscriptions sub-info))

  (add-publication [messenger pub-info]
    (update-in messenger
               [:publications (:dst-task-id pub-info) (:slot-id pub-info)]
               (fn [pbs] 
                 (assert (= id (:src-peer-id pub-info)) [id (:src-peer-id pub-info)] )
                 (conj (or pbs []) 
                       (new-publication messenger pub-info)))))

  (remove-publication [messenger pub-info]
    (.close ^Publication (:publication pub-info))
    (update messenger :publications remove-from-publications pub-info))

  (set-replica-version [messenger replica-version]
    (reset! (:read-index messenger) 0)
    (run! (fn [sub] (reset! (:barrier-ack sub) nil)) ack-subscriptions)
    (run! (fn [sub] (reset! (:barrier sub) nil)) subscriptions)
    (-> messenger 
        (assoc :replica-version replica-version)
        (m/set-epoch 0)))

  (replica-version [messenger]
    (get messenger :replica-version))

  (epoch [messenger]
    epoch)

  (set-epoch [messenger epoch]
    (assoc messenger :epoch epoch))

  (next-epoch [messenger]
    (update messenger :epoch inc))

  (poll-acks [messenger]
    (reduce poll-acks! messenger ack-subscriptions))

  (all-acks-seen? 
    [messenger]
    (if (empty? (remove (comp deref :barrier-ack) ack-subscriptions))
      (select-keys @(:barrier-ack (first ack-subscriptions)) 
                   [:replica-version :epoch])))

  (flush-acks [messenger]
    (run! (fn [sub] (reset! (:barrier-ack sub) nil)) ack-subscriptions)
    messenger)

  (poll [messenger]
    (let [subscriber (get subscriptions (mod @(:read-index messenger) (count subscriptions)))
          messages (poll-messages! messenger subscriber)] 
      (swap! (:read-index messenger) inc)
      (mapv t/input messages)))

  (offer-segments [messenger batch {:keys [dst-task-id slot-id] :as task-slot}]
    ;; Problem here is that if no slot will accept the message we will
    ;; end up having to recompress on the next offer
    ;; Possibly should try more than one iteration before returning
    ;; TODO: should re-use unsafe buffers in aeron messenger. 
    ;; Will require nippy to be able to write directly to unsafe buffers
    (let [payload ^bytes (messaging-compress (->Message id dst-task-id slot-id replica-version batch))
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      ;; shuffle publication order to ensure even coverage
      ;; FIXME: slow
      (loop [pubs (shuffle (get-in publications [dst-task-id slot-id]))]
        (if-let [pub-info (first pubs)]
          (let [ret (.offer ^Publication (:publication pub-info) buf 0 (.capacity buf))]
            (if (neg? ret)
              (recur (rest pubs))
              task-slot))))))

  (poll-recover [messenger]
    (loop [sbs subscriptions]
      (let [sub (first sbs)] 
        (when sub 
          (when-not @(:barrier sub)
            (poll-new-replica! messenger sub))
          (recur (rest sbs)))))
    ;(info "ON REC" id (m/all-barriers-seen? messenger))
    (if (m/all-barriers-seen? messenger)
      (let [_ (info "ALL SEEN SUBS " (vec subscriptions))
            recover (:recover @(:barrier (first subscriptions)))] 
        (assert (= 1 (count (set (map (comp :recover deref :barrier) subscriptions)))))
        (assert recover)
        recover)))

  (emit-barrier [messenger publication]
    (onyx.messaging.messenger/emit-barrier messenger publication {}))

  (emit-barrier [messenger publication barrier-opts]
    (let [barrier (merge (->Barrier id (:dst-task-id publication) (m/replica-version messenger) (m/epoch messenger))
                         barrier-opts)
          publication ^Publication (:publication publication)
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress barrier))]
      (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
        (println "barrier ret" ret)
        (if (neg? ret)
          :fail
          :success))))

  (unblock-subscriptions! [messenger]
    (run! set-barrier-emitted! subscriptions)
    messenger)

  (all-barriers-seen? [messenger]
    (empty? (remove #(found-next-barrier? messenger %) 
                    subscriptions)))

  (emit-barrier-ack [messenger publication]
    (let [ack (->BarrierAck id 
                            (:dst-task-id publication) 
                            (m/replica-version messenger) 
                            (m/epoch messenger))
          publication ^Publication (:publication publication)
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress ack))]
      (let [ret (.offer ^Publication publication buf 0 (.capacity buf))] 
        (println "ack ret" ret)
        (if (neg? ret)
          :fail
          :success)))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (map->AeronMessenger {:id id 
                        :peer-config peer-config 
                        :messenger-group messenger-group 
                        :read-index (atom 0)}))

(defmethod clojure.core/print-method AeronMessagingPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
