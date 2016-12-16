(ns onyx.messaging.aeron.status-publisher
  (:require [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.types :refer [barrier? message? heartbeat? ->Heartbeat ->ReadyReply]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [io.aeron Aeron Aeron$Context Publication]))

;; FIXME, centralise
(def UNALIGNED_SUBSCRIBER -11)

(deftype StatusPublisher [peer-config peer-id dst-peer-id site ^Aeron conn ^Publication pub 
                          ^:unsynchronized-mutable blocked ^:unsynchronized-mutable completed
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
          pub (.addPublication conn channel heartbeat-stream-id)
          initial-heartbeat (System/currentTimeMillis)]
      (StatusPublisher. peer-config peer-id dst-peer-id site conn pub 
                        blocked completed nil initial-heartbeat)))
  (stop [this]
    (.close conn)
    (try
     (when pub (.close pub))
     (catch io.aeron.exceptions.RegistrationException re
       (info "Error closing publication from status publisher" re)))
    (StatusPublisher. peer-config peer-id dst-peer-id site nil nil false false nil nil))
  (info [this]
    {:INFO :TODO})
  (get-session-id [this]
    session-id)
  (set-session-id! [this sess-id]
    (assert (or (nil? session-id) (= session-id sess-id)))
    (set! session-id sess-id)
    this)
  (set-heartbeat! [this]
    (set! heartbeat (System/currentTimeMillis))
    this)
  (get-heartbeat [this]
    heartbeat)
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
  (new-replica-version! [this]
    (set! blocked false)
    (set! completed false)
    this)
  (offer-barrier-status! [this replica-version epoch]
    (if session-id 
      ;; Maybe want a heartbeat boolean to say whether it's the first barrier status
      (let [barrier-aligned (->Heartbeat replica-version epoch peer-id dst-peer-id session-id)
            payload ^bytes (messaging-compress barrier-aligned)
            buf ^UnsafeBuffer (UnsafeBuffer. payload)
            ret (.offer ^Publication pub buf 0 (.capacity buf))]
        (assert session-id)
        (debug "Offered barrier status message:" [ret barrier-aligned :session-id (.sessionId pub) :dst-site site])
        ret) 
      UNALIGNED_SUBSCRIBER))
  (offer-ready-reply! [this replica-version epoch]
    (let [ready-reply (->ReadyReply replica-version peer-id dst-peer-id session-id)
          payload ^bytes (messaging-compress ready-reply)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication pub buf 0 (.capacity buf))] 
      (debug "Offer ready reply!:" [ret ready-reply :session-id (.sessionId pub) :dst-site site]))))

(defn new-status-publisher [peer-config peer-id src-peer-id site]
  (->StatusPublisher peer-config peer-id src-peer-id site nil nil false false nil nil))
