(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.handler :as handler]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.types :refer [barrier? message? ->Heartbeat ->ReadyReply]]
            [taoensso.timbre :refer [warn] :as timbre])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [io.aeron Aeron Aeron$Context Publication Subscription ControlledFragmentAssembler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]))

;; FIXME to be tuned
(def fragment-limit-receiver 10000)

(deftype RecoverFragmentHandler 
  [src-peer-id 
   dst-task-id 
   slot-id
   session-id
   heartbeats
   ^:unsynchronized-mutable replica-version 
   ^:unsynchronized-mutable recover]
  handler/PartialSubscriber
  (set-replica-version! [this new-replica-version]
    (reset! session-id nil)
    (assert new-replica-version)
    (assert (or (nil? replica-version) (> new-replica-version replica-version)))
    (set! replica-version new-replica-version)
    (set! recover nil)
    this)
  (prepare-poll! [this]
    this)
  (get-recover [this] 
    recover)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          rv-msg (:replica-version message)
          ret (cond (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (and (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id)
                             ;; TODO, currently hash on slot-id anyway
                             ;; Ready doesn't have slot-id in it though
                             #_(not= (:slot-id message) slot-id)
                             
                             )
                         ;; I think we can even skip > replica-version because it 
                         ;; should have checked whether ready
                         (= rv-msg replica-version))
                    ControlledFragmentHandler$Action/CONTINUE

                    (and (instance? onyx.types.Ready message)
                         (= rv-msg replica-version))
                    (do 
                     (assert (or (nil? @session-id) (= @session-id (.sessionId header))))
                     (reset! session-id (.sessionId header))
                     (println "Received another ready message" message)
                     ControlledFragmentHandler$Action/CONTINUE)

                    (and (barrier? message)
                         (= replica-version (:replica-version message))
                         (= 1 (:epoch message)))
                    (do 
                     (assert (:recover message))
                     (set! recover (:recover message))
                     ControlledFragmentHandler$Action/BREAK)

                    (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/ABORT

                    :else
                    (throw (ex-info "Recover handler should never be here." {:message message})))]
      (println [:recover (action->kw ret) dst-task-id src-peer-id :rv replica-version :session-id (.sessionId header) :pos (.position header)] message)
      ret)))

;; TODO, can skip everything if it's the wrong image completely

(deftype ReadSegmentsFragmentHandler 
  [src-peer-id dst-task-id slot-id session-id heartbeats ^:unsynchronized-mutable blocked 
   ^:unsynchronized-mutable ticket ^:unsynchronized-mutable batch 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch 
   ^:unsynchronized-mutable completed]
  handler/PartialSubscriber
  (set-replica-version! [this new-replica-version]
    (assert (or (nil? replica-version) (> new-replica-version replica-version)))
    (set! replica-version new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (prepare-poll! [this]
    (set! batch (transient []))
    this)
  (set-ticket! [this new-ticket]
    (set! ticket new-ticket)
    this)
  (get-batch [this]
    (persistent! batch))
  (blocked? [this] 
    blocked)
  (unblock! [this]
    (set! blocked false)
    this)
  (block! [this]
    (set! blocked false)
    this)
  (completed? [this]
    completed)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (if-not (= (.sessionId header) @session-id)
      ;; Should we not skip over later replica data here?
      ;; I don't think it matters because of ready status
      ControlledFragmentHandler$Action/CONTINUE
      (let [ba (byte-array length)
            _ (.getBytes ^UnsafeBuffer buffer offset ba)
            message (messaging-decompress ba)
            ;; FIXME, why 2?
            n-desired-messages 2
            position (.position header)
            rv-msg (:replica-version message)
            ret (cond (and (or (not= (:dst-task-id message) dst-task-id)
                               (not= (:src-peer-id message) src-peer-id)
                               ;; TODO, currently hash on slot-id anyway
                               ;; Ready doesn't have slot-id in it though
                               #_(not= (:slot-id message) slot-id))
                           (= rv-msg replica-version))
                      ControlledFragmentHandler$Action/CONTINUE

                      (< rv-msg replica-version)
                      ControlledFragmentHandler$Action/CONTINUE

                      (> rv-msg replica-version)
                      (throw (ex-info "Shouldn't happen as we have not sent our ready message." 
                                      {:replica-version replica-version 
                                       :message message}))

                      (>= (count batch) n-desired-messages)
                      ControlledFragmentHandler$Action/ABORT

                      (message? message)
                      (let [ticket-val @ticket] 
                        (if (and (< ticket-val position)
                                 (compare-and-set! ticket ticket-val position))
                          (do
                           ;; FIXME, not sure if this logically works.
                           ;; If ticket gets updated in mean time, then is this always invalid and should be continued?
                           ;; WORK OUT ON PAPER
                           (assert (= replica-version rv-msg))
                           (println "Got payload" (:payload message) "ticket-val" ticket-val 
                                    (.sessionId header))
                           (reduce conj! batch (:payload message)))
                          (println "Skipped over due to ticket-val read" message ticket-val position
                                   (.sessionId header)))
                        ;; Continue no matter what. 
                        ;; We have either read the message, or it was read by another peer.
                        ControlledFragmentHandler$Action/CONTINUE)

                      (barrier? message)
                      (do
                       (when-not (= (inc epoch) (:epoch message))
                         (throw (ex-info "Unexpected barrier found. Possibly a misaligned subscription."
                                         {:message message
                                          :epoch epoch
                                          :replica replica-version})))
                       ;(println "Got barrier " message)
                       (if (zero? (count batch)) ;; empty? broken on transients
                         (do 
                          (set! blocked true)
                          ;; For use determining whether job is complete. Refactor later
                          (when (:completed? message)
                            (set! completed true))
                          ControlledFragmentHandler$Action/BREAK)  
                         ControlledFragmentHandler$Action/ABORT))

                      :else
                      (throw (ex-info "Should not happen?"
                                      {:replica-version replica-version
                                       :epoch epoch
                                       :position position
                                       :message message})))]
        (println [:handle-message (action->kw ret) dst-task-id src-peer-id] (.position header) message)
        ret))))

(deftype Subscriber 
  [messenger messenger-group job-id src-peer-id dst-task-id slot-id site src-site stream-id 
   ;; Maybe these should be final and setup in the new-fn
   ^:unsynchronized-mutable ^Aeron conn 
   ^:unsynchronized-mutable ^Subscription subscription 
   ^:unsynchronized-mutable recover-handler
   ^:unsynchronized-mutable recover-assembler
   ^:unsynchronized-mutable segments-handler
   ^:unsynchronized-mutable segments-assembler
   ^:unsynchronized-mutable status-conn
   ^:unsynchronized-mutable status-pub
   ^:unsynchronized-mutable session-id]
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
                  ;; make these do stuff to whether it's ready or not?
                  #_(.availableImageHandler (available-image sub-info))
                  #_(.unavailableImageHandler (unavailable-image sub-info)))
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
          sess-id (atom nil)
          heartbeats (atom {})
          recover-fragment-handler (->RecoverFragmentHandler src-peer-id dst-task-id slot-id sess-id heartbeats nil nil)
          recover-fragment-assembler (ControlledFragmentAssembler. recover-fragment-handler)
          segments-fragment-handler (->ReadSegmentsFragmentHandler src-peer-id dst-task-id slot-id sess-id heartbeats nil nil nil nil nil nil)
          segments-fragment-assembler (ControlledFragmentAssembler. segments-fragment-handler)]
      ;; FIXME TODO, should close everything we've opened so far in here if we catch an exception in start. Then we rethrow
      (set! recover-handler recover-fragment-handler)
      (set! recover-assembler recover-fragment-assembler)
      (set! segments-handler segments-fragment-handler)
      (set! segments-assembler segments-fragment-assembler)
      (set! session-id sess-id)
      (set! conn conn*)
      (set! subscription sub)
      (set! status-conn status-conn*)
      (set! status-pub status-pub*)
      (println "New sub:" (sub/sub-info this))
      this))
  (stop [this]
    (try (.close subscription)
         (catch Throwable t
           (warn t "Error closing subscription.")))
    (try (.close conn)
         (catch Throwable t
           (warn t "Error closing subscription conn.")))
    (try (.close status-pub)
         (catch Throwable t
           (warn t "Error closing subscription heartbeat publication.")))
    (try (.close status-conn)
         (catch Throwable t
           (warn t "Error closing subscription heartbeat publication conn.")))
    this)
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
    (handler/set-epoch! segments-handler new-epoch)
    this)
  (set-replica-version! [this new-replica-version]
    (handler/set-replica-version! segments-handler new-replica-version)
    (handler/set-replica-version! recover-handler new-replica-version)
    (handler/block! segments-handler)
    this)
  (get-recover [this]
    (handler/get-recover recover-handler))
  (unblock! [this]
    (handler/unblock! segments-handler)
    this)
  (blocked? [this]
    (handler/blocked? segments-handler))
  (completed? [this]
    (handler/completed? segments-handler))
  ;; Check heartbeat code should also check whether all pubs and subs are closed
  ;;(check-heartbeats [this])
  (poll-messages! [this]
    (if-not (sub/blocked? this)
      (let [_ (handler/prepare-poll! segments-handler)
            _ (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler segments-assembler fragment-limit-receiver)
            batch (handler/get-batch segments-handler)]
        (println "polled batch:" batch)
        batch)
      []))
  (offer-heartbeat! [this]
    (let [msg (->Heartbeat (m/replica-version messenger) (m/id messenger) @session-id)
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      (.offer ^Publication status-pub buf 0 (.capacity buf))))
  (offer-ready-reply! [this]
    (let [ready-reply (->ReadyReply (m/replica-version messenger) (m/id messenger) @session-id)
          payload ^bytes (messaging-compress ready-reply)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication status-pub buf 0 (.capacity buf))] 
      (println "Offer ready reply!:" [ret ready-reply])))
  (poll-replica! [this]
    (println "poll-replica!, sub-info:" (sub/sub-info this))
    ;; TODO, should check heartbeats
    (handler/prepare-poll! recover-handler)
    (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler recover-assembler fragment-limit-receiver)
    (when @session-id 
      (handler/set-ticket! segments-handler (m/lookup-ticket messenger src-peer-id @session-id))
      (sub/offer-ready-reply! this))))

(defn new-subscription [messenger messenger-group sub-info]
  (let [{:keys [job-id src-peer-id dst-task-id slot-id site src-site]} sub-info
        s-id (stream-id job-id dst-task-id slot-id site)] 
    (->Subscriber messenger messenger-group job-id src-peer-id dst-task-id slot-id site src-site
                  s-id nil nil nil nil nil nil nil nil nil)))

(defn reconcile-sub [messenger messenger-group subscriber sub-info]
  (if-let [sub (cond (and subscriber (nil? sub-info))
                     (do (sub/stop subscriber)
                         nil)
                     (and (nil? subscriber) sub-info)
                     (sub/start (new-subscription messenger messenger-group sub-info))
                     :else
                     subscriber)]
    (sub/set-replica-version! sub (m/replica-version messenger))))
