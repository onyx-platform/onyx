(ns onyx.messaging.aeron.endpoint-status
  (:require [onyx.messaging.protocols.endpoint-status]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :refer [heartbeat-stream-id]]
            [onyx.messaging.protocols.endpoint-status]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent UnsafeBuffer]))

;; FIXME to be tuned
(def fragment-limit-receiver 10000)

(deftype EndpointStatus 
  [messenger-group session-id ^:unsynchronized-mutable conn 
   ^:unsynchronized-mutable subscription ^:unsynchronized-mutable replica-version 
   ^:unsynchronized-mutable peers ^:unsynchronized-mutable ready-peers 
   ^:unsynchronized-mutable heartbeats ^:unsynchronized-mutable ready]
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
          conn* (Aeron/connect ctx)
          channel (common/aeron-channel (:bind-addr messenger-group) (:port messenger-group))
          sub (.addSubscription conn* channel heartbeat-stream-id)]
      (set! conn conn*)
      (set! subscription sub)
      this))
  (stop [this]
    (.close subscription)
    (.close conn)
    this)
  (poll! [this]
    (.poll ^Subscription subscription ^FragmentHandler this fragment-limit-receiver))
  (set-heartbeat-peers! [this expected-peers]
    (set! peers expected-peers))
  (ready? [this]
    (assert (or ready (not= ready-peers peers)))
    ready)
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (set! ready false)
    (set! replica-version new-replica-version)
    (set! ready-peers #{})
    (set! heartbeats {})
    this)
  FragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          msg-rv (:replica-version message)
          msg-sess (:session-id message)]
      (println "Got heartbeat message" message)
      (when (and (= session-id msg-sess)
                 (= replica-version msg-rv))
        (cond (instance? onyx.types.ReadyReply message)
              (let [peer-id (:src-peer-id message)
                    new-ready (conj ready-peers peer-id)] 
                (set! ready-peers new-ready)
                (set! heartbeats (assoc heartbeats peer-id (System/currentTimeMillis)))
                (println "PUB: ready-peer" peer-id "session-id" session-id)
                (when (= ready-peers peers)
                  (println "PUB: all peers ready.")
                  (set! ready true)))
              (instance? onyx.types.Heartbeat message)
              (let [peer-id (:src-peer-id message)] 
                (println (format "PUB: peer heartbeat: %s. Time since last heartbeat: %s." 
                                 peer-id (if-let [t (get heartbeats peer-id)] 
                                           (- (System/currentTimeMillis) t)
                                           :never)))
                (set! heartbeats (assoc heartbeats peer-id (System/currentTimeMillis)))))))))

(defn new-endpoint-status [messenger-group session-id]
  (->EndpointStatus messenger-group session-id nil nil nil nil nil nil false)) 
