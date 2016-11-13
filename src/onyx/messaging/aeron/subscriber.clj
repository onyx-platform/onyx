(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.handler :as handler]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.types :refer [barrier? message? heartbeat? ->Heartbeat ->ReadyReply]]
            [taoensso.timbre :refer [warn] :as timbre])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [io.aeron Aeron Aeron$Context Publication Subscription ControlledFragmentAssembler]
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

(deftype ReadSegmentsFragmentHandler 
  [src-peer-id dst-task-id slot-id ticket-counters 
   ^:unsynchronized-mutable heartbeat
   ^:unsynchronized-mutable session-id 
   ^:unsynchronized-mutable recover 
   ^:unsynchronized-mutable blocked 
   ^:unsynchronized-mutable ^AtomicLong ticket 
   ^:unsynchronized-mutable batch 
   ^:unsynchronized-mutable replica-version 
   ^:unsynchronized-mutable epoch 
   ^:unsynchronized-mutable completed]
  handler/Handler
  (set-replica-version! [this new-replica-version]
    (assert (or (nil? replica-version) (> new-replica-version replica-version)))
    ;; can move this to a non atom
    (set! replica-version new-replica-version)
    (set! heartbeat nil)
    (set! session-id nil)
    (set! recover nil)
    this)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (get-session-id [this]
    session-id)
  (prepare-poll! [this]
    (set! batch (transient []))
    this)
  (get-heartbeat [this]
    heartbeat)
  (set-heartbeat! [this]
    (set! heartbeat (System/currentTimeMillis))
    this)
  (set-recover! [this recover*]
    (assert recover*)
    (set! recover recover*)
    (handler/set-heartbeat! this)
    this)
  (get-recover [this] 
    recover)
  (get-batch [this]
    (persistent! batch))
  (blocked? [this] 
    blocked)
  (unblock! [this]
    (set! blocked false)
    this)
  (block! [this]
    (set! blocked true)
    this)
  (set-ready! [this sess-id]
    (assert (or (nil? session-id) (= sess-id session-id)))
    (set! session-id sess-id)
    (set! ticket (lookup-ticket ticket-counters src-peer-id sess-id))
    (handler/set-heartbeat! this))
  (completed? [this]
    completed)
  (handle-recovery [this message sess-id]
    (let [ret (cond (< (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (> (:replica-version message) replica-version)
                    ControlledFragmentHandler$Action/ABORT

                    (and (instance? onyx.types.Ready message)
                         (= (:replica-version message) replica-version))
                    (do (handler/set-ready! this sess-id)
                        ControlledFragmentHandler$Action/CONTINUE)

                    ;; Ignore heartbeats for now? Probably shouldn't heartbeat unless we're aligned?
                    ;; Or should we have different standards?
                    (heartbeat? message)
                    ControlledFragmentHandler$Action/CONTINUE

                    (and (or (barrier? message)
                             (message? message))
                         (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id)
                             (not= (:slot-id message) slot-id)))
                    ControlledFragmentHandler$Action/CONTINUE

                    (and (barrier? message) 
                         (= 1 (:epoch message)))
                    (do (handler/set-recover! this (:recover message))
                        ControlledFragmentHandler$Action/BREAK)

                    :else
                    (throw (ex-info "Recover handler should never be here." {:message message})))]
      (println [:recover (action->kw ret) dst-task-id src-peer-id] message)
      ret))
  (handle-messages [this message position]
    (let [;; FIXME, why 2?
          n-desired-messages 2
          rv-msg (:replica-version message)
          ret (cond (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (> rv-msg replica-version)
                    (throw (ex-info "Shouldn't happen as we have not sent our ready message." 
                                    {:replica-version replica-version 
                                     :message message}))

                    (heartbeat? message)
                    (do (handler/set-heartbeat! this)
                        ControlledFragmentHandler$Action/CONTINUE)

                    (message? message)
                    (cond (and (or (not= (:dst-task-id message) dst-task-id)
                                   (not= (:src-peer-id message) src-peer-id)
                                   (not= (:slot-id message) slot-id)))
                          ControlledFragmentHandler$Action/CONTINUE

                          ;; full batch, get out
                          (>= (count batch) n-desired-messages)
                          ControlledFragmentHandler$Action/ABORT

                          :else
                          (let [ticket-val ^long (.get ticket)
                                assigned? (and (< ticket-val position)
                                               (.compareAndSet ticket ticket-val position))]
                            (when assigned?
                              (reduce conj! batch (:payload message)))
                            (handler/set-heartbeat! this)
                            ControlledFragmentHandler$Action/CONTINUE))

                    (barrier? message)
                    (cond (and (or (not= (:dst-task-id message) dst-task-id)
                                   (not= (:src-peer-id message) src-peer-id)
                                   (not= (:slot-id message) slot-id)))
                          ControlledFragmentHandler$Action/CONTINUE

                          (zero? (count batch))
                          ;; move into own method?
                          (do
                           (assert-epoch-correct! epoch (:epoch message) message)
                           ;; For use determining whether job is complete. Refactor later
                           (-> this 
                               (handler/set-heartbeat!)
                               (handler/block!))
                           (when (:completed? message)
                             (set! completed true))
                           ControlledFragmentHandler$Action/BREAK)

                          :else
                          ControlledFragmentHandler$Action/ABORT)

                    ;; skip extras
                    (instance? onyx.types.Ready message)
                    ControlledFragmentHandler$Action/CONTINUE

                    :else
                    (throw (ex-info "Should not happen?"
                                    {:replica-version replica-version
                                     :epoch epoch
                                     :position position
                                     :message message})))]
      (println [:handle-message (action->kw ret) dst-task-id src-peer-id] position message)
      ret))

  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (cond (not recover)
          (let [ba (byte-array length)
                _ (.getBytes ^UnsafeBuffer buffer offset ba)
                message (messaging-decompress ba)]
            (handler/handle-recovery this message (.sessionId header)))

          ;; src-peer-id will not be messaging us on an image with a different session-id
          (not= (.sessionId header) session-id)
          ControlledFragmentHandler$Action/CONTINUE

          :else
          (let [ba (byte-array length)
                _ (.getBytes ^UnsafeBuffer buffer offset ba)
                message (messaging-decompress ba)]
            (handler/handle-messages this message (.position header))))))

(deftype Subscriber 
  [messenger messenger-group job-id src-peer-id dst-task-id slot-id site src-site stream-id 
   ^Aeron conn ^Subscription subscription handler assembler status-conn status-pub]
  sub/Subscriber
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (println "Aeron messaging subscriber error" x)
                            (System/exit 1)
                            ;; FIXME: Reboot peer
                            (taoensso.timbre/warn x "Aeron messaging subscriber error")))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler)
                  ;(.availableImageHandler (available-image sub-info))
                  ;(.unavailableImageHandler (unavailable-image sub-info))
                  )
          conn* (Aeron/connect ctx)
          bind-addr (:bind-addr messenger-group)
          port (:port messenger-group)
          channel (common/aeron-channel bind-addr port)
          sub (.addSubscription conn* channel stream-id)
          status-channel (common/aeron-channel (:address src-site) (:port src-site))
          status-ctx (-> (Aeron$Context.)
                         (.errorHandler error-handler))
          status-conn* (Aeron/connect status-ctx)
          status-pub* (.addPublication status-conn* status-channel heartbeat-stream-id)
          fragment-handler (->ReadSegmentsFragmentHandler src-peer-id dst-task-id slot-id (m/ticket-counters messenger) nil nil nil nil nil nil nil nil nil)
          fragment-assembler (ControlledFragmentAssembler. fragment-handler)]
      (Subscriber. messenger messenger-group job-id src-peer-id dst-task-id slot-id site src-site stream-id 
                   conn* sub fragment-handler fragment-assembler status-conn* status-pub*)))
  (stop [this]
    (when subscription (.close subscription))
    (when conn (.close conn))
    (when status-pub (.close status-pub))
    (when status-conn (.close status-conn))
    (Subscriber. messenger messenger-group job-id src-peer-id dst-task-id slot-id site src-site stream-id nil nil nil nil nil nil))
  (key [this]
    [src-peer-id dst-task-id slot-id site])
  (sub-info [this]
    [:rv (m/replica-version messenger)
     :e (m/epoch messenger)
     :blocked? (sub/blocked? this)
     :channel-id (.channel subscription)
     :registation-id (.registrationId subscription)
     :stream-id (.streamId subscription)
     :closed? (.isClosed subscription)
     :images (mapv (fn [i] [:pos (.position i) 
                            :term-id (.initialTermId i) 
                            :session-id (.sessionId i) 
                            :closed? (.isClosed i) 
                            :corr-id (.correlationId i) 
                            :source-id (.sourceIdentity i)]) 
                   (.images subscription)) 
     :sub-info {:src-peer-id src-peer-id 
                :dst-task-id dst-task-id 
                :slot-id slot-id 
                :site site}])
  ;; TODO: add send heartbeat
  ;; This should be a separate call, only done once per task lifecycle, and also check when blocked
  (equiv-meta [this sub-info]
    (and (= src-peer-id (:src-peer-id sub-info))
         (= dst-task-id (:dst-task-id sub-info))
         (= slot-id (:slot-id sub-info))
         (= site (:site sub-info))))
  (set-epoch! [this new-epoch]
    (handler/set-epoch! handler new-epoch)
    this)
  (set-replica-version! [this new-replica-version]
    (handler/set-replica-version! handler new-replica-version)
    (handler/block! handler)
    this)
  (get-recover [this]
    (handler/get-recover handler))
  (unblock! [this]
    (handler/unblock! handler)
    this)
  (blocked? [this]
    (handler/blocked? handler))
  (completed? [this]
    (handler/completed? handler))
  (poll-messages! [this]
    (if-not (sub/blocked? this)
      (let [_ (handler/prepare-poll! handler)
            _ (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler assembler fragment-limit-receiver)
            batch (handler/get-batch handler)]
        (println "polled batch:" batch)
        batch)
      []))
  (offer-heartbeat! [this]
    (let [msg (->Heartbeat (m/replica-version messenger) (m/id messenger) (handler/get-session-id handler))
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      (.offer ^Publication status-pub buf 0 (.capacity buf))))
  (offer-ready-reply! [this]
    (let [ready-reply (->ReadyReply (m/replica-version messenger) (m/id messenger) (handler/get-session-id handler))
          payload ^bytes (messaging-compress ready-reply)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication status-pub buf 0 (.capacity buf))] 
      (println "Offer ready reply!:" [ret ready-reply])))
  (poll-replica! [this]
    (println "poll-replica!, sub-info:" (sub/sub-info this))
    (println "latest heartbeat is" (handler/get-heartbeat handler))
    ;; TODO, should check heartbeats
    (handler/prepare-poll! handler)
    (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler assembler fragment-limit-receiver)
    ;; if we have a session id, we are ready, and since we haven't found the 
    ;; replica lets notify the publisher that we're ready.
    (when (handler/get-session-id handler) 
      (sub/offer-ready-reply! this))))

(defn new-subscription [messenger messenger-group sub-info]
  (let [{:keys [job-id src-peer-id dst-task-id slot-id site src-site]} sub-info
        s-id (stream-id job-id dst-task-id slot-id site)] 
    (->Subscriber messenger messenger-group job-id src-peer-id dst-task-id slot-id 
                  site src-site s-id nil nil nil nil nil nil)))

(defn reconcile-sub [messenger messenger-group subscriber sub-info]
  (if-let [sub (cond (and subscriber (nil? sub-info))
                     (do (sub/stop subscriber)
                         nil)

                     (and (nil? subscriber) sub-info)
                     (sub/start (new-subscription messenger messenger-group sub-info))

                     :else
                     subscriber)]
    (sub/set-replica-version! sub (m/replica-version messenger))))
