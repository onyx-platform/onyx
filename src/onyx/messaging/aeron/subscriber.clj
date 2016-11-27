(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.types :refer [barrier? message? heartbeat? ->Heartbeat ->ReadyReply ->BarrierAlignedDownstream]]
            [taoensso.timbre :refer [info warn] :as timbre])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [io.aeron Aeron Aeron$Context Publication Subscription Image ControlledFragmentAssembler UnavailableImageHandler AvailableImageHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]))

;; FIXME to be tuned
(def fragment-limit-receiver 1000)

(defn ^java.util.concurrent.atomic.AtomicLong lookup-ticket [ticket-counters src-peer-id session-id]
  (-> ticket-counters
      (swap! update-in 
             [src-peer-id session-id]
             (fn [ticket]
               (or ticket (AtomicLong. -1))))
      (get-in [src-peer-id session-id])))

(defn assert-epoch-correct! [epoch message-epoch message]
  (when-not (= (inc epoch) message-epoch)
    (throw (ex-info "Unexpected barrier found. Possibly a misaligned subscription."
                    {:message message
                     :epoch epoch}))))

(defn invalid-replica-found! [replica-version message]
  (throw (ex-info "Shouldn't have received a message for this replica-version as we have not sent a ready message." 
                  {:replica-version replica-version 
                   :message message})))

(defn unavailable-image [sub-info]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (info "UNAVAILABLE image " (.position image) " " (.sessionId image) " " sub-info))))

(defn available-image [sub-info]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (info "AVAILABLE image " (.position image) " " (.sessionId image) " " sub-info))))

;; One subscriber has multiple status pubs, one for each publisher
;; this moves the reconciliation into the subscriber itself
;; Have a status publisher type
;; Containing src-peer / src-site

(deftype StatusPublisher [peer-config peer-id dst-peer-id site ^Aeron conn ^Publication pub 
                          ^:unsynchronized-mutable session-id ^:unsynchronized-mutable heartbeat]
  status-pub/PStatusPublisher
  (start [this]
    (let [media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          status-error-handler (reify ErrorHandler
                                 (onError [this x] 
                                   (info "Aeron status channel error" x)
                                   ;(System/exit 1)
                                   ;; FIXME: Reboot peer
                                   (taoensso.timbre/warn x "Aeron status channel error")))
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler status-error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          channel (autil/channel (:address site) (:port site))
          conn (Aeron/connect ctx)
          pub (.addPublication conn channel heartbeat-stream-id)]
      (StatusPublisher. peer-config peer-id dst-peer-id site conn pub nil nil)))
  (stop [this]
    (when conn (.close conn))
    (when pub (.close pub))
    (StatusPublisher. peer-config peer-id dst-peer-id site nil nil nil nil))
  (info [this]
    {:INFO :TODO})
  (set-session-id! [this sess-id]
    (assert (or (nil? session-id) (= session-id sess-id)))
    (set! session-id sess-id)
    this)
  (set-heartbeat! [this]
    (set! heartbeat (System/currentTimeMillis))
    this)
  (offer-heartbeat! [this replica-version epoch]
    (let [msg (->Heartbeat replica-version peer-id epoch)
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))] 
      (info "Offer heartbeat subscriber:" [ret msg :session-id (.sessionId pub) :dst-site site])))
  (offer-ready-reply! [this replica-version epoch]
    (let [ready-reply (->ReadyReply replica-version peer-id dst-peer-id session-id)
          payload ^bytes (messaging-compress ready-reply)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))] 
      (info "Offer ready reply!:" [ret ready-reply :session-id (.sessionId pub) :dst-site site])))
  (offer-barrier-aligned! [this replica-version epoch]
    (let [barrier-aligned (->BarrierAlignedDownstream replica-version epoch peer-id dst-peer-id session-id)
          payload ^bytes (messaging-compress barrier-aligned)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))]
      (info "Offered barrier aligned message message:" [ret barrier-aligned :session-id (.sessionId pub) :dst-site site])
      ret)))

(defn new-status-publisher [peer-config peer-id src-peer-id site]
  (->StatusPublisher peer-config peer-id src-peer-id site nil nil nil nil))

(deftype Subscriber 
  [peer-id ticket-counters peer-config dst-task-id slot-id site sources 
   liveness-timeout channel ^Aeron conn ^Subscription subscription status-pubs
   ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler 
   ^:unsynchronized-mutable replica-version 
   ^:unsynchronized-mutable epoch
   ^:unsynchronized-mutable heartbeat
   ^:unsynchronized-mutable recover 
   ^:unsynchronized-mutable is-recovered 
   ^:unsynchronized-mutable blocked 
   ;; Going to need to be ticket per source, session-id per source, etc
   ;^:unsynchronized-mutable ^AtomicLong ticket 
   ^:unsynchronized-mutable batch 
   ^:unsynchronized-mutable completed]
  sub/Subscriber
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (println "Aeron messaging subscriber error" x)
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (taoensso.timbre/warn x "Aeron messaging subscriber error")))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          sinfo [dst-task-id slot-id :sources sources :to site]
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir)
                true (.availableImageHandler (available-image sinfo))
                true (.unavailableImageHandler (unavailable-image sinfo)))
          conn (Aeron/connect ctx)
          liveness-timeout (arg-or-default :onyx.peer/publisher-liveness-timeout-ms peer-config)
          channel (autil/channel peer-config)
          stream-id (stream-id dst-task-id slot-id site)
          sub (.addSubscription conn channel stream-id)
          status-pubs (->> sources
                           (map (fn [{:keys [src-peer-id site]}]
                                  [src-peer-id (status-pub/start (new-status-publisher peer-config peer-id src-peer-id site))]))
                           (into {}))]
      (sub/add-assembler 
       (Subscriber. peer-id ticket-counters peer-config dst-task-id
                    slot-id site sources liveness-timeout channel conn sub
                    status-pubs nil nil nil nil nil nil nil nil nil)))) 
  (stop [this]
    (info "Stopping subscriber" [dst-task-id slot-id site])
    (when subscription (.close subscription))
    (when conn (.close conn))
    (run! status-pub/stop (vals status-pubs))
    (Subscriber. peer-id ticket-counters peer-config dst-task-id slot-id site sources 
                 nil nil nil nil nil nil nil nil nil nil nil nil nil nil)) 
  (key [this]
    ;; IMPORTANT: this should match onyx.messaging.aeron.utils/stream-id keys
    [dst-task-id slot-id site])
  (add-assembler [this]
    (set! assembler (ControlledFragmentAssembler. this))
    this)
  (info [this]
    {:subscription {:rv replica-version
                    :e epoch
                    :sources sources 
                    :dst-task-id dst-task-id 
                    :slot-id slot-id 
                    :blocked? (sub/blocked? this)
                    :recovered? (sub/recovered? this)
                    :site site
                    :channel (autil/channel peer-config)
                    :channel-id (.channel subscription)
                    :registation-id (.registrationId subscription)
                    :stream-id (.streamId subscription)
                    :closed? (.isClosed subscription)
                    :images (mapv autil/image->map (.images subscription))}
     :status-pubs (into {} (map (fn [[k v]] [k (status-pub/info v)]) status-pubs))})
  ;; TODO: add send heartbeat
  ;; This should be a separate call, only done once per task lifecycle, and also check when blocked
  (alive? [this]
    true
    #_(and (not (.isClosed subscription))
         (> (+ (sub/get-heartbeat this) liveness-timeout)
            (System/currentTimeMillis))))
  (equiv-meta [this sub-info]
    (and ;(= src-peer-id (:src-peer-id sub-info))
         (= dst-task-id (:dst-task-id sub-info))
         (= slot-id (:slot-id sub-info))
         (= site (:site sub-info))))
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (set-replica-version! [this new-replica-version]
    (set! replica-version new-replica-version)
    (set! heartbeat nil)
    (set! recover nil)
    (set! is-recovered false)
    (set! blocked false)
    this)
  (recovered? [this]
    is-recovered)
  (get-recover [this]
    recover)
  (set-recovered! [this]
    (set! is-recovered true)
    this)
   (set-recover! [this recover*]
     (assert recover*)
     (set! recover recover*)
     this)
  (prepare-poll! [this]
    (set! batch (transient []))
    this)
  ;; NEXT REMOVE BLOCK AND UNBLOCK!
  (unblock! [this]
    (set! blocked false)
    this)
  (block! [this]
    (set! blocked true)
    this)
  (blocked? [this]
    blocked)
  (completed? [this]
    completed)
  (poll-messages! [this]
    (info "Poll messages on channel" (autil/channel peer-config) "blocked" (sub/blocked? this))
    ;; TODO: Still needs to read on this!!!!
    ;; Just like poll replica, but can't read actual messages, but shouldn't be receiving them anyway
    (assert assembler)
    (let [_ (sub/prepare-poll! this)
          _ (info "Before poll" (sub/info this))
          rcv (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler assembler fragment-limit-receiver)]
      (info "After poll" (sub/info this))
      (persistent! batch)))
  (offer-heartbeat! [this]
    (run! #(status-pub/offer-heartbeat! % replica-version epoch) (vals status-pubs)))
  (offer-barrier-aligned! [this peer-id]
    (let [status-pub (get status-pubs peer-id)] 
      (status-pub/offer-barrier-aligned! status-pub replica-version epoch)))
  (src-peers [this]
    (keys status-pubs))
  (set-heartbeat! [this src-peer-id]
    (status-pub/set-heartbeat! (get status-pubs src-peer-id))
    this)
  (poll-replica! [this]
    (info "poll-replica!, sub-info:" (sub/info this))
    ;; TODO, should check heartbeats
    (sub/prepare-poll! this)
    (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler assembler fragment-limit-receiver))
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          n-desired-messages 2
          ret (cond (< (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    ;; Should this ever happen? Guess maybe if it's lagging?
                    ;; Leave in for now
                    (> (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/ABORT

                    (instance? onyx.types.Ready message)
                    (let [src-peer-id (:src-peer-id message)] 
                      (-> status-pubs
                          (get src-peer-id)
                          (status-pub/set-session-id! (.sessionId header))
                          (status-pub/offer-ready-reply! replica-version epoch))
                      (sub/set-heartbeat! this src-peer-id)
                      ControlledFragmentHandler$Action/CONTINUE)

                    ;; Can we skip over these even for the wrong replica version? 
                    ;; Probably since we would be sending a ready anyway
                    ;; This would prevent lagging peers blocking
                    (and (or (barrier? message)
                             (message? message))
                         (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:slot-id message) slot-id)
                             ;; We're not caring about this src-peer-id
                             ;; I don't think this check is necessary
                             (not (get status-pubs (:src-peer-id message)))))
                    ControlledFragmentHandler$Action/CONTINUE

                    (heartbeat? message)
                    (do
                     (sub/set-heartbeat! this (:src-peer-id message))
                     ControlledFragmentHandler$Action/CONTINUE)

                    (barrier? message)
                    (if (zero? (count batch))
                      (do
                       (assert-epoch-correct! epoch (:epoch message) message)
                       ;; For use determining whether job is complete. Refactor later
                       (sub/set-heartbeat! this (:src-peer-id message))
                       (-> this 
                           (sub/block!))
                       (some->> message :recover (sub/set-recover! this))
                       (when (:completed? message) (set! completed true))
                       ControlledFragmentHandler$Action/BREAK)
                      ControlledFragmentHandler$Action/ABORT)

                    (message? message)
                    (if (>= (count batch) n-desired-messages) ;; full batch, get out
                      ControlledFragmentHandler$Action/ABORT
                      (let [_ (assert (pos? epoch))
                            session-id (.sessionId header)
                            src-peer-id (:src-peer-id message)
                            ticket (lookup-ticket ticket-counters src-peer-id session-id)
                            ticket-val ^long (.get ticket)
                            position (.position header)
                            assigned? (and (< ticket-val position)
                                           (.compareAndSet ticket ticket-val position))]
                        (when assigned?
                          (reduce conj! batch (:payload message)))
                        (sub/set-heartbeat! this (:src-peer-id message))
                        ControlledFragmentHandler$Action/CONTINUE))

                    :else
                    (throw (ex-info "Handler should never be here." {:replica-version replica-version
                                                                     :epoch epoch
                                                                     :message message})))]
      (info [:recover (action->kw ret) channel dst-task-id] (into {} message))
      ret)))

(defn new-subscription [peer-config peer-id ticket-counters sub-info]
  (let [{:keys [dst-task-id slot-id site sources]} sub-info]
    (->Subscriber peer-id ticket-counters peer-config dst-task-id slot-id site
                  sources nil nil nil nil nil nil nil nil nil nil nil nil nil nil)))

(defn reconcile-sub [peer-config peer-id ticket-counters subscriber sub-info]
  (cond (and subscriber (nil? sub-info))
        (do (sub/stop subscriber)
            nil)

        (and (nil? subscriber) sub-info)
        (sub/start (new-subscription peer-config peer-id ticket-counters sub-info))

        :else
        subscriber))
