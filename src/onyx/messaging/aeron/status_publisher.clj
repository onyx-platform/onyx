(ns onyx.messaging.aeron.status-publisher
  (:require [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.peer.constants :refer [UNALIGNED_SUBSCRIBER]]
            [onyx.types :as t]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id 
                                                          try-close-conn try-close-publication]]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [io.aeron Aeron Aeron$Context Publication]
           [java.util.concurrent.atomic AtomicLong]))

(deftype StatusPublisher 
  [peer-config
   peer-id
   dst-peer-id
   site
   ^Boolean short-circuit?
   ^UnsafeBuffer buffer
   ^Aeron conn
   ^Publication pub
   ^:unsynchronized-mutable ^AtomicLong ticket
   ^:unsynchronized-mutable short-circuit
   ^:unsynchronized-mutable blocked
   ^:unsynchronized-mutable completed
   ^:unsynchronized-mutable checkpoint
   ^:unsynchronized-mutable watermarks
   ^:unsynchronized-mutable short-id
   ^:unsynchronized-mutable session-id
   ^:unsynchronized-mutable heartbeat]
  status-pub/PStatusPublisher
  (start [this]
    (let [media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          status-error-handler (reify ErrorHandler
                                 (onError [this x] 
                                   (taoensso.timbre/warn x "Aeron status channel error")))
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler status-error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          channel (autil/channel (:address site) (:port site))
          conn (Aeron/connect ctx)
          pub (.addPublication conn channel heartbeat-stream-id)
          initial-heartbeat (System/nanoTime)]
      (StatusPublisher. peer-config peer-id dst-peer-id site short-circuit? buffer conn pub 
                        nil nil blocked completed checkpoint watermarks nil nil initial-heartbeat)))
  (stop [this]
    (info "Closing status pub" (status-pub/info this))
    (some-> pub try-close-publication)
    (some-> conn try-close-conn)
    (StatusPublisher. peer-config peer-id dst-peer-id site short-circuit? buffer 
                      nil nil nil nil nil false false false nil nil nil))
  (info [this]
    (let [dst-channel (autil/channel (:address site) (:port site))] 
      {:type :status-publisher
       :src-peer-id peer-id
       :dst-peer-id dst-peer-id
       :status-session-id session-id
       :short-id short-id
       :site site
       :dst-channel dst-channel
       :dst-session-id (.sessionId pub) 
       :stream-id (.streamId pub)
       :blocked? blocked
       :completed? completed
       :checkpoint? checkpoint
       :pos (.position pub)}))
  (get-session-id [this]
    session-id)
  (set-session-id! [this session-id* ticket* short-circuit*]
    (assert (or (nil? session-id) (= session-id session-id*)))
    (set! ticket ticket*)
    (set! session-id session-id*)
    (set! short-circuit short-circuit*)
    this)
  (set-short-id! [this short-id*]
    (set! short-id short-id*)
    this)
  (get-short-id [this] short-id)
  (set-heartbeat! [this]
    (set! heartbeat (System/nanoTime))
    this)
  (get-heartbeat [this]
    heartbeat)
  (get-ticket [this]
    ticket)
  (get-short-circuit [this]
    (if short-circuit?
      short-circuit))
  (block! [this]
    (assert (false? blocked))
    (set! blocked true)
    this)
  (unblock! [this]
    (set! blocked false))
  (blocked? [this]
    blocked)
  (set-completed! [this completed?]
    (set! completed completed?))
  (completed? [this]
    completed)
  (set-checkpoint! [this checkpoint?]
    (assert (not (nil? checkpoint?)))
    (set! checkpoint checkpoint?))
  (set-watermarks! [this watermarks*]
    (set! watermarks (merge watermarks watermarks*)))
  (watermarks [this]
    watermarks)
  (checkpoint? [this]
    checkpoint)
  (new-replica-version! [this]
    (set! blocked false)
    (set! completed false)
    this)
  (offer-barrier-status! [this replica-version epoch opts]
    (if session-id 
      (let [barrier-aligned (merge (t/heartbeat replica-version epoch peer-id dst-peer-id session-id short-id) opts)
            len (sz/serialize buffer 0 barrier-aligned)
            ret (.offer ^Publication pub buffer 0 len)]
        (debug "Offered barrier status message:" [ret barrier-aligned (status-pub/info this)])
        ret) 
      UNALIGNED_SUBSCRIBER))
   (offer-ready-reply! [this replica-version epoch]
      (let [ready-reply (t/ready-reply replica-version peer-id dst-peer-id session-id short-id) 
            len (sz/serialize buffer 0 ready-reply)
            ret (.offer ^Publication pub buffer 0 len)] 
        (debug "Offer ready reply!:" [ret ready-reply (status-pub/info this)])
        ret)))

(defn new-status-publisher [peer-config peer-id src-peer-id site]
  (let [short-circuit? (autil/short-circuit? peer-config site)
        buf (UnsafeBuffer. (byte-array t/max-control-message-size))] 
    (->StatusPublisher peer-config peer-id src-peer-id site short-circuit?
                       buf nil nil nil nil nil false false {} false nil nil)))
