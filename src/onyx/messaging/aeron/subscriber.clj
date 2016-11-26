(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.handler :as handler]
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
           [onyx.messaging.protocols.handler Handler]
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

(deftype StatusPublisher [peer-config peer-id dst-peer-id site ^Aeron conn ^Publication pub]
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
      (StatusPublisher. peer-config peer-id dst-peer-id site conn pub)))
  (stop [this]
    (when conn (.close conn))
    (when pub (.close pub)))
  (offer-heartbeat! [this replica-version epoch]
    (let [msg (->Heartbeat replica-version peer-id epoch)
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))] 
      (info "Offer heartbeat subscriber:" [ret msg :session-id (.sessionId pub) :dst-site site])))
  (offer-ready-reply! [this replica-version epoch session-id]
    (let [ready-reply (->ReadyReply replica-version peer-id dst-peer-id session-id)
          payload ^bytes (messaging-compress ready-reply)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))] 
      (info "Offer ready reply!:" [ret ready-reply :session-id (.sessionId pub) :dst-site site])))
  (offer-barrier-aligned! [this replica-version epoch session-id]
    (let [barrier-aligned (->BarrierAlignedDownstream replica-version epoch peer-id dst-peer-id session-id)
          payload ^bytes (messaging-compress barrier-aligned)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))]
      (info "Offered barrier aligned message message:" [ret barrier-aligned :session-id (.sessionId pub) :dst-site site])
      ret)))

(deftype Subscriber 
  [peer-id ticket-counters peer-config dst-task-id slot-id site sources 
   liveness-timeout channel ^Aeron conn ^Subscription subscription status-pubs
   ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler 
   ^:unsynchronized-mutable replica-version 
   ^:unsynchronized-mutable epoch
   ^:unsynchronized-mutable heartbeat
   ^:unsynchronized-mutable session-id 
   ^:unsynchronized-mutable recover 
   ^:unsynchronized-mutable is-recovered 
   ^:unsynchronized-mutable blocked 
   ;; Going to need to be ticket per source, session-id per source, etc
   ^:unsynchronized-mutable ^AtomicLong ticket 
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
          ;; Rename status-pub to status-pubs
          status-pubs (->> sources
                           (map (fn [{:keys [src-peer-id site]}]
                                  [src-peer-id
                                   (status-pub/start (->StatusPublisher peer-config peer-id src-peer-id site nil nil))]))
                           (into {}))]
      (sub/add-assembler 
       (Subscriber. peer-id ticket-counters peer-config dst-task-id
                    slot-id site sources liveness-timeout channel conn sub
                    status-pubs nil nil nil nil nil nil nil nil nil nil nil)))) 
  (stop [this]
    (info "Stopping subscriber" [dst-task-id slot-id site])
    (when subscription (.close subscription))
    (when conn (.close conn))
    (run! status-pub/stop (vals status-pubs))
    (Subscriber. peer-id ticket-counters peer-config dst-task-id slot-id site sources 
                 nil nil nil nil nil nil nil nil nil nil nil nil nil nil nil nil)) 
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
                    :rev-image (some->> (.images subscription)
                                        (filter 
                                         (fn [^Image i]
                                           (= (sub/get-session-id this) (.sessionId i))))
                                        (first)
                                        (autil/image->map))
                    :images (mapv autil/image->map (.images subscription))}
     ; :status-pub {:channel (autil/channel (:address src-site) (:port src-site))
     ;              :session-id (.sessionId status-pub) 
     ;              :stream-id (.streamId status-pub)
     ;              :pos (.position status-pub)}

     })
  ;; TODO: add send heartbeat
  ;; This should be a separate call, only done once per task lifecycle, and also check when blocked
  (alive? [this]
    (and (not (.isClosed subscription))
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
    (set! session-id nil)
    (set! recover nil)
    (set! is-recovered false)
    (set! blocked false)
    this)
  (get-session-id [this]
    session-id)
  (recovered? [this]
    is-recovered)
  (get-heartbeat [this]
    heartbeat)
  (set-heartbeat! [this]
    (set! heartbeat (System/currentTimeMillis))
    this)
  (get-recover [this]
    recover)
  (set-recovered! [this]
    (set! is-recovered true)
    this)
   (set-recover! [this recover*]
     (assert recover*)
     (set! recover recover*)
     (sub/set-heartbeat! this)
     this)
  (prepare-poll! [this]
    (set! batch (transient []))
    this)
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
  (offer-ready-reply! [this]
    (run! (fn [status-pub] 
            ;; Move session-id into status-pubs
            (status-pub/offer-ready-reply! status-pub replica-version epoch (sub/get-session-id this)))
          (vals status-pubs)))
  (offer-barrier-aligned! [this]
    ;; Going to need to be able to track status publishers in here
    (run! (fn [status-pub] 
            ;; Move session-id into status-pubs
            (status-pub/offer-barrier-aligned! status-pub replica-version epoch (sub/get-session-id this)))
          (vals status-pubs))
    ;; URGGHHH
    ;; URGGHHH
    ;; URGGHHH
    ;; URGGHHH
    1
    
    )
  (poll-replica! [this]
    (info "poll-replica!, sub-info:" (sub/info this))
    (info "latest heartbeat is" (sub/get-heartbeat this))
    ;; TODO, should check heartbeats
    (sub/prepare-poll! this)
    (let [rcv (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler assembler fragment-limit-receiver)]
      ;; if we have a session id, we are ready, and since we haven't found the 
      ;; replica lets notify the publisher that we're ready.
      (when (sub/get-session-id this) 
        (sub/offer-ready-reply! this))))
  (set-ready! [this sess-id]
    (assert (or (nil? session-id) (= sess-id session-id)))
    (set! session-id sess-id)
    (let [src-peer-id (:src-peer-id (first sources))] 
      (assert src-peer-id)
      (set! ticket (lookup-ticket ticket-counters src-peer-id sess-id)))
    (sub/set-heartbeat! this))
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          n-desired-messages 2
          rv-msg (:replica-version message)
          ;; Hard code to single peer for now
          src-peer-id (:src-peer-id (first sources))
          _ (assert src-peer-id)
          ret (cond (instance? onyx.types.Ready message)
                    (do 
                     (when (= (:src-peer-id message) src-peer-id)
                       (sub/set-ready! this (.sessionId header)))
                        ControlledFragmentHandler$Action/CONTINUE)

                    ;; Can we skip over these even for the wrong replica version? 
                    ;; Probably since we would be sending a ready anyway
                    ;; This would prevent lagging peers blocking
                    (and (or (barrier? message)
                             (message? message))
                         (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id)
                             (not= (:slot-id message) slot-id)))
                    ControlledFragmentHandler$Action/CONTINUE

                    (< (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (> (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/ABORT

                    (heartbeat? message)
                    (do
                     (sub/set-heartbeat! this)
                     ControlledFragmentHandler$Action/CONTINUE)

                    (barrier? message)
                    (if (zero? (count batch))
                      (do
                       (assert-epoch-correct! epoch (:epoch message) message)
                       ;; For use determining whether job is complete. Refactor later
                       (-> this 
                           (sub/set-heartbeat!)
                           (sub/block!))
                       (some->> message :recover (sub/set-recover! this))
                       (when (:completed? message)
                         (set! completed true))
                       ControlledFragmentHandler$Action/BREAK)
                      ControlledFragmentHandler$Action/ABORT)

                    (message? message)
                    (do
                     (assert (pos? epoch))
                     (if ;; full batch, get out
                       (>= (count batch) n-desired-messages)
                       ControlledFragmentHandler$Action/ABORT
                       (let [ticket-val ^long (.get ticket)
                             position (.position header)
                             assigned? (and (< ticket-val position)
                                            (.compareAndSet ticket ticket-val position))]
                         (when assigned?
                           (reduce conj! batch (:payload message)))
                         (sub/set-heartbeat! this)
                         ControlledFragmentHandler$Action/CONTINUE)))

                    :else
                    (throw (ex-info "Recover handler should never be here." {:message message})))]
      (info [:recover (action->kw ret) channel dst-task-id src-peer-id] (into {} message))
      ret)))

(defn new-subscription [peer-config peer-id ticket-counters sub-info]
  (let [{:keys [dst-task-id slot-id site sources]} sub-info]
    (->Subscriber peer-id ticket-counters peer-config dst-task-id slot-id site
                  sources nil nil nil nil nil nil nil nil nil nil nil nil nil nil nil nil)))

(defn reconcile-sub [peer-config peer-id ticket-counters subscriber sub-info]
  (cond (and subscriber (nil? sub-info))
        (do (sub/stop subscriber)
            nil)

        (and (nil? subscriber) sub-info)
        (sub/start (new-subscription peer-config peer-id ticket-counters sub-info))

        :else
        subscriber))
