(ns onyx.messaging.aeron.endpoint-status
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.peer.constants :refer [initialize-epoch]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns]]
            [taoensso.timbre :refer [debug info warn trace] :as timbre])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent UnsafeBuffer]))

;; FIXME to be tuned
(def fragment-limit-receiver 1000)

(deftype EndpointStatus 
  [peer-config peer-id session-id liveness-timeout-ns ^Aeron conn ^Subscription subscription 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch 
   ^:unsynchronized-mutable endpoint-peers ^:unsynchronized-mutable ready-peers 
   ^:unsynchronized-mutable peer-epochs 
   ^:unsynchronized-mutable min-epochs-downstream 
   ^:unsynchronized-mutable heartbeats 
   ^:unsynchronized-mutable ready]
  onyx.messaging.protocols.endpoint-status/EndpointStatus
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging heartbeat error" x)
                            (taoensso.timbre/warn "Aeron messaging heartbeat error:" x)))
          media-driver-dir ^String (:onyx.messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName media-driver-dir))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          sub (.addSubscription conn channel heartbeat-stream-id)
          liveness-timeout-ns (ms->ns (arg-or-default :onyx.peer/subscriber-liveness-timeout-ms 
                                                      peer-config))]
      (info "Started endpoint status on peer:" peer-id)
      (EndpointStatus. peer-config peer-id session-id liveness-timeout-ns conn
                       sub replica-version epoch endpoint-peers ready-peers
                       peer-epochs min-epochs-downstream heartbeats ready)))
  (stop [this]
    (info "Stopping endpoint status" [peer-id])
    (try
     (.close subscription)
     (catch Throwable t
       (info "Error closing endpoint subscription:" t)))
     (.close conn)
    (EndpointStatus. peer-config peer-id session-id nil nil nil nil nil nil nil nil nil nil false))
  (info [this]
    [:rv replica-version
     :e epoch
     :channel-id (.channel subscription)
     :registration-id (.registrationId subscription)
     :stream-id (.streamId subscription)
     :closed? (.isClosed subscription)
     :images (mapv autil/image->map (.images subscription)) 
     :endpoint-peers endpoint-peers
     :epochs-downstream peer-epochs
     :heartbeats heartbeats
     :ready? ready])
  (poll! [this]
    ;; FIXME MUST HANDLE LOST IMAGES HERE AS WE MAY HAVE LOST REPLICA MESSAGES
    ;; WE CAN GET AROUND IT BY INCLUDING EPOCH IN HEARTBEAT.
    (debug "Polling endpoint status" peer-id 
           "channel" (autil/channel peer-config) 
           (onyx.messaging.protocols.endpoint-status/info this))
    (.poll ^Subscription subscription ^FragmentHandler this fragment-limit-receiver))
  (set-endpoint-peers! [this expected-peers]
    (set! endpoint-peers expected-peers)
    (->> expected-peers
         (map (fn [p] [p initialize-epoch]))
         (into {})
         (set! peer-epochs)
         (set! min-epochs-downstream))
    this)
  (ready? [this]
    (assert (or ready (not= ready-peers endpoint-peers)))
    ready)
  (timed-out-subscribers [this]
    (let [curr-time (System/nanoTime)]
      (sequence (comp (filter (fn [[peer-id heartbeat]] 
                                (< (+ heartbeat liveness-timeout-ns)
                                   curr-time)))
                      (map key))
                heartbeats)))
  (min-endpoint-epoch [this]
    ;; TODO: do we actually care about the max? The max is what tells us what is actually available
    ;; At the endpoint, though it is not as a good backpressure, as other peers may be lagging
    ;; Replace with a version that actually updates this on each message coming in
    (apply min (vals peer-epochs)))
  (min-downstream-epoch [this]
    (apply min (vals min-epochs-downstream)))
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (set! replica-version new-replica-version)
    (set! ready false)
    (set! ready-peers #{})
    (set! heartbeats {})
    this)
  (set-epoch! [this new-epoch]
    (assert new-epoch)
    (set! epoch new-epoch)
    this)
  FragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          msg-rv (:replica-version message)
          msg-sess (:session-id message)]
      (debug "EndpointStatusRead, ignore?" 
             session-id msg-sess
             (not (and (= session-id msg-sess) (= replica-version msg-rv))) 
             "message" (type message) (into {} message))
      ;; We only care about the ready reply or heartbeat if it is for us, 
      ;; and it is only valid if it is for the same replica version that we are on
      (when (and (= session-id msg-sess) (= replica-version msg-rv))
        (case (int (:type message))
          2 (when (= peer-id (:dst-peer-id message))
              (let [src-peer-id (:src-peer-id message)
                    epoch (:epoch message)
                    prev-epoch (get peer-epochs src-peer-id)]
                (debug (format "PUB: peer heartbeat: %s. Time since last heartbeat: %s." 
                              peer-id (if-let [t (get heartbeats peer-id)] 
                                        (- (System/nanoTime) t)
                                        :never)))
                (set! heartbeats (assoc heartbeats src-peer-id (System/nanoTime)))
                (set! min-epochs-downstream (assoc min-epochs-downstream src-peer-id (:min-epoch message)))
                (debug "Barrier aligned message" (into {} message))
                (cond (= epoch (inc prev-epoch))
                      (set! peer-epochs (assoc peer-epochs src-peer-id epoch))
                      (= epoch prev-epoch)
                      (do
                       (debug "Got heartbeat at peer:" peer-id (into {} message))
                       :heartbeat)
                      :else
                      (throw (ex-info "Received epoch is not in sync with expected epoch." 
                                      {:epoch epoch
                                       :prev-epoch prev-epoch
                                       :message message})))))
          4 (when (= peer-id (:dst-peer-id message))
              (let [src-peer-id (:src-peer-id message)] 
                (debug "Read ReadyReply" message)
                (set! ready-peers (conj ready-peers src-peer-id))
                (when (= ready-peers endpoint-peers)
                  (set! ready true))
                (set! heartbeats (assoc heartbeats src-peer-id (System/nanoTime)))
                (debug "PUB: ready-peer" src-peer-id "session-id" session-id)
                (debug "PUB: all peers ready?" (= ready-peers endpoint-peers) ready-peers "vs" endpoint-peers)))
          (throw (ex-info "Invalid message type" {:message message})))))))

(defn new-endpoint-status [peer-config peer-id session-id]
  (->EndpointStatus peer-config peer-id session-id nil nil nil nil nil nil nil nil nil nil false)) 
