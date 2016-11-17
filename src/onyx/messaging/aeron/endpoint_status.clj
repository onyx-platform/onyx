(ns onyx.messaging.aeron.endpoint-status
  (:require [onyx.messaging.protocols.endpoint-status]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.protocols.endpoint-status]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent UnsafeBuffer]))

;; FIXME to be tuned
(def fragment-limit-receiver 1000)

(deftype EndpointStatus 
  [peer-config peer-id session-id liveness-timeout conn subscription 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch 
   ^:unsynchronized-mutable endpoint-peers ^:unsynchronized-mutable ready-peers 
   ^:unsynchronized-mutable epochs-downstream ^:unsynchronized-mutable heartbeats ^:unsynchronized-mutable ready]
  onyx.messaging.protocols.endpoint-status/EndpointStatus
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging heartbeat error" x)
                            (taoensso.timbre/warn "Aeron messaging heartbeat error:" x)))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          sub (.addSubscription conn channel heartbeat-stream-id)
          liveness-timeout (arg-or-default :onyx.peer/subscriber-liveness-timeout-ms peer-config)]
      (EndpointStatus. peer-config peer-id session-id liveness-timeout conn sub replica-version epoch 
                       endpoint-peers ready-peers epochs-downstream heartbeats ready)))
  (stop [this]
    (.close subscription)
    (.close conn)
    (EndpointStatus. peer-config peer-id session-id nil nil nil nil nil nil nil nil nil false))
  (poll! [this]
    (println "Polling endpoint status" )
    (.poll ^Subscription subscription ^FragmentHandler this fragment-limit-receiver))
  (set-endpoint-peers! [this expected-peers]
    (set! endpoint-peers expected-peers)
    (set! epochs-downstream (into {} (map (fn [p] [p 0]) expected-peers))))
  (ready? [this]
    (assert (or ready (not= ready-peers endpoint-peers)))
    ready)
  (liveness [this]
    (let [curr-time (System/currentTimeMillis)] 
      (->> heartbeats
           (map (fn [[peer-id heartbeat]]
                  [peer-id (> (+ heartbeat liveness-timeout) curr-time)]))
           (into {}))))
  (min-endpoint-epoch [this]
    ;; TODO: do we actually care about the max? The max is what tells us what is actually available
    ;; At the endpoint, though it is not as a good backpressure, as other peers may be lagging
    ;; Replace with a version that actually updates this on each message coming in
    (println "Epochs downstream" epochs-downstream)
    (apply min (vals epochs-downstream)))
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
      ;; We only care about the ready reply or heartbeat if it is for us, 
      ;; and it is only valid if it is for the same replica version that we are on
      (when (and (= session-id msg-sess) (= replica-version msg-rv))
        (cond (instance? onyx.types.Heartbeat message)
              (let [peer-id (:src-peer-id message)] 
                (println (format "PUB: peer heartbeat: %s. Time since last heartbeat: %s." 
                                 peer-id (if-let [t (get heartbeats peer-id)] 
                                           (- (System/currentTimeMillis) t)
                                           :never)))
                (set! heartbeats (assoc heartbeats peer-id (System/currentTimeMillis))))

              (instance? onyx.types.ReadyReply message)
              (when (= peer-id (:dst-peer-id message))
                (let [src-peer-id (:src-peer-id message)] 
                  (println "Read ReadyReply" message)
                  (set! ready-peers (conj ready-peers src-peer-id))
                  (set! heartbeats (assoc heartbeats src-peer-id (System/currentTimeMillis)))
                  (println "PUB: ready-peer" src-peer-id "session-id" session-id)
                  (println "PUB: all peers ready?" (= ready-peers endpoint-peers) ready-peers "vs" endpoint-peers)
                  (when (= ready-peers endpoint-peers)
                    (set! ready true))))

              (instance? onyx.types.BarrierAlignedDownstream message)
              (when (= peer-id (:dst-peer-id message))
                (println "Barrier aligned message" message)
                (set! epochs-downstream (assoc epochs-downstream (:src-peer-id message) (:epoch message))))

              :else
              (throw (ex-info "Invalid message type" {:message message})))))))

(defn new-endpoint-status [peer-config peer-id session-id]
  (->EndpointStatus peer-config peer-id session-id nil nil nil nil nil nil nil nil nil false)) 
