(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.aeron.status-publisher :refer [new-status-publisher]]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.types :refer [barrier? message? heartbeat? ->Heartbeat ->ReadyReply]]
            [taoensso.timbre :refer [info warn] :as timbre])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [io.aeron Aeron Aeron$Context Publication Subscription Image ControlledFragmentAssembler UnavailableImageHandler AvailableImageHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]))

;; FIXME to be tuned
(def fragment-limit-receiver 10000)

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

(defn unavailable-image [sub-info lost-sessions]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (swap! lost-sessions conj (.sessionId image))
      (println "Lost sessions now" @lost-sessions)
      (info "UNAVAILABLE image " (.position image) " " (.sessionId image) " " sub-info))))

(defn available-image [sub-info]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (info "AVAILABLE image " (.position image) " " (.sessionId image) " " sub-info))))

(deftype Subscriber 
  [peer-id ticket-counters peer-config dst-task-id slot-id site batch-size
   liveness-timeout channel ^Aeron conn ^Subscription subscription 
   lost-sessions 
   ^:unsynchronized-mutable sources
   ^:unsynchronized-mutable status-pubs
   ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler 
   ^:unsynchronized-mutable replica-version 
   ^:unsynchronized-mutable epoch
   ^:unsynchronized-mutable recover 
   ;; Going to need to be ticket per source, session-id per source, etc
   ;^:unsynchronized-mutable ^AtomicLong ticket 
   ^:unsynchronized-mutable batch]
  sub/Subscriber
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (println "Aeron messaging subscriber error" x)
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (taoensso.timbre/warn x "Aeron messaging subscriber error")))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          sinfo [dst-task-id slot-id site]
          lost-sessions (atom #{})
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir)
                true (.availableImageHandler (available-image sinfo))
                true (.unavailableImageHandler (unavailable-image sinfo lost-sessions)))
          conn (Aeron/connect ctx)
          liveness-timeout (arg-or-default :onyx.peer/publisher-liveness-timeout-ms peer-config)
          channel (autil/channel peer-config)
          stream-id (stream-id dst-task-id slot-id site)
          sub (.addSubscription conn channel stream-id)]
      (sub/add-assembler 
       (Subscriber. peer-id ticket-counters peer-config dst-task-id
                    slot-id site batch-size liveness-timeout channel conn sub lost-sessions 
                    [] {} nil nil nil nil nil)))) 
  (stop [this]
    (info "Stopping subscriber" [dst-task-id slot-id site])
    ;; Possible issue here when closing. Should hard exit? Or should just safely close more
    ;; Can trigger this with really short timeouts
    ;; ^[[1;31mio.aeron.exceptions.RegistrationException^[[m: ^[[3mUnknown subscription link: 78^[[m
    ;; ^[[1;31mjava.util.concurrent.ExecutionException^[[m: ^[[3mio.aeron.exceptions.RegistrationException: Unknown subscription link: 78^[[m
    (when subscription 
      (try
       (.close subscription)
       (catch io.aeron.exceptions.RegistrationException re
         (info "ERR" re))))
    (when conn (.close conn))
    (run! status-pub/stop (vals status-pubs))
    (Subscriber. peer-id ticket-counters peer-config dst-task-id slot-id site 
                 batch-size nil nil nil nil nil nil nil nil nil nil nil nil)) 
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
                    :site site
                    :channel (autil/channel peer-config)
                    :channel-id (.channel subscription)
                    :registation-id (.registrationId subscription)
                    :stream-id (.streamId subscription)
                    :closed? (.isClosed subscription)
                    :images (mapv autil/image->map (.images subscription))}
     :status-pubs (into {} (map (fn [[k v]] [k (status-pub/info v)]) status-pubs))})
  ;; FIXME: RENAME
  (timed-out-publishers [this]
    (let [curr-time (System/currentTimeMillis)] 
      (->> status-pubs
           (filter (fn [[peer-id spub]] 
                     (< (+ (status-pub/get-heartbeat spub)
                           liveness-timeout)
                        curr-time)))
           (map key))))
  ;; TODO: add send heartbeat
  ;; This should be a separate call, only done once per task lifecycle, and also check when blocked
  (alive? [this]
    (cond (.isClosed subscription)
          :closed
          (< (+ (apply min 
                       (map status-pub/get-heartbeat 
                            (vals status-pubs)))
                liveness-timeout)
             (System/currentTimeMillis))
          :no-heartbeat
          (let [status-pub-session-ids (set (keep status-pub/get-session-id (vals status-pubs)))
                lost-valid (clojure.set/intersection @lost-sessions status-pub-session-ids)]
            (not (empty? lost-valid))) 
          :interrupted-session
          :else
          :alive))
  (equiv-meta [this sub-info]
    (and (= dst-task-id (:dst-task-id sub-info))
         (= slot-id (:slot-id sub-info))
         (= site (:site sub-info))))
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (set-replica-version! [this new-replica-version]
    (run! status-pub/new-replica-version! (vals status-pubs))
    (set! replica-version new-replica-version)
    (set! recover nil)
    this)
  (get-recover [this]
    recover)
  (set-recover! [this recover*]
    (assert recover*)
    (when-not (or (nil? recover) (= recover* recover)) 
      (throw (ex-info "Two different subscribers sent differing recovery information"
                      {:recover1 recover
                       :recover2 recover*
                       :replica-version replica-version
                       :epoch epoch})))
    (set! recover recover*)
    this)
  (unblock! [this]
    (run! status-pub/unblock! (vals status-pubs))
    this)
  (blocked? [this]
    (not (some (complement status-pub/blocked?) (vals status-pubs))))
  (completed? [this]
    (not (some (complement status-pub/completed?) (vals status-pubs))))
  (poll! [this]
    (info "Before poll on channel" (sub/info this))
    (set! batch (transient []))
    (let [rcv (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler assembler fragment-limit-receiver)]
      (info "After poll" (sub/info this))
      (persistent! batch)))
  (offer-heartbeat! [this]
    (run! #(status-pub/offer-barrier-status! % replica-version epoch) (vals status-pubs)))
  (offer-barrier-status! [this peer-id]
    (let [status-pub (get status-pubs peer-id)] 
      (status-pub/offer-barrier-status! status-pub replica-version epoch)))
  (src-peers [this]
    (keys status-pubs))
  (update-sources! [this sources*]
    (let [prev-peer-ids (set (keys status-pubs))
          next-peer-ids (set (map :src-peer-id sources*))
          peer-id->site (into {} (map (juxt :src-peer-id :site) sources*))
          rm-peer-ids (clojure.set/difference prev-peer-ids next-peer-ids)
          add-peer-ids (clojure.set/difference next-peer-ids prev-peer-ids)
          removed (reduce (fn [spubs src-peer-id]
                            (status-pub/stop (get spubs src-peer-id))
                            (dissoc spubs src-peer-id))
                          status-pubs
                          rm-peer-ids)
          added (reduce (fn [spubs src-peer-id]
                          (->> (get peer-id->site src-peer-id)
                               (new-status-publisher peer-config peer-id src-peer-id)
                               (status-pub/start)
                               (assoc spubs src-peer-id)))
                        removed
                        add-peer-ids)]
      (set! status-pubs added)
      (set! sources sources*))
    this)
  (set-heartbeat! [this src-peer-id]
    (status-pub/set-heartbeat! (get status-pubs src-peer-id))
    this)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          ret (cond (< (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    ;; TODO: may not ever happen due to pull semantics
                    (> (:replica-version message) replica-version)
                    (do
                     ;; update heartbeat since we're blocked and it's not
                     ;; upstream's fault
                     (-> status-pubs
                         (get (:src-peer-id message))
                         (status-pub/set-heartbeat!))
                     ControlledFragmentHandler$Action/ABORT)

                    (instance? onyx.types.Ready message)
                    (let [src-peer-id (:src-peer-id message)] 
                      (-> status-pubs
                          (get src-peer-id)
                          (status-pub/set-heartbeat!)
                          (status-pub/set-session-id! (.sessionId header))
                          (status-pub/offer-ready-reply! replica-version epoch))
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
                     (info "Received heartbeat" (:src-peer-id message))
                     (sub/set-heartbeat! this (:src-peer-id message))
                     ControlledFragmentHandler$Action/CONTINUE)

                    (barrier? message)
                    (if (zero? (count batch))
                      (let [src-peer-id (:src-peer-id message)
                            status-pub (get status-pubs src-peer-id)]
                        (assert-epoch-correct! epoch (:epoch message) message)
                        ;; For use determining whether job is complete. Refactor later
                        (sub/set-heartbeat! this src-peer-id)
                        (status-pub/block! status-pub)
                        (when (:completed? message) 
                          (status-pub/set-completed! status-pub (:completed? message)))
                        (some->> message :recover (sub/set-recover! this))
                        ControlledFragmentHandler$Action/BREAK)
                      ControlledFragmentHandler$Action/ABORT)

                    (message? message)
                    (if (>= (count batch) batch-size) ;; full batch, get out
                      ControlledFragmentHandler$Action/ABORT
                      (let [_ (assert (pos? epoch))
                            _ (sub/set-heartbeat! this (:src-peer-id message))
                            session-id (.sessionId header)
                            src-peer-id (:src-peer-id message)
                            ;; TODO: slow
                            ticket (lookup-ticket ticket-counters src-peer-id session-id)
                            ticket-val ^long (.get ticket)
                            position (.position header)
                            assigned? (and (< ticket-val position)
                                           (.compareAndSet ticket ticket-val position))]
                        (when assigned? (reduce conj! batch (:payload message)))
                        ControlledFragmentHandler$Action/CONTINUE))

                    :else
                    (throw (ex-info "Handler should never be here." {:replica-version replica-version
                                                                     :epoch epoch
                                                                     :message message})))]
      (info [:read-subscriber (action->kw ret) channel dst-task-id] (into {} message))
      ret)))

(defn new-subscription [peer-config peer-id ticket-counters sub-info]
  (let [{:keys [dst-task-id slot-id site batch-size]} sub-info]
    (->Subscriber peer-id ticket-counters peer-config dst-task-id slot-id site batch-size
                  nil nil nil nil nil nil nil nil nil nil nil nil)))
