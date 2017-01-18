(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.peer.constants :refer [NOT_READY ENDPOINT_BEHIND]]
            [onyx.types :refer [heartbeat ready]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [io.aeron Aeron Aeron$Context Publication UnavailableImageHandler AvailableImageHandler]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]))

;; Need last heartbeat check time so we don't have to check everything too frequently?
;; TODO, BIG PREALLOCATED BUFFER TO PUT EVERYTHING IN
;; WITH REPLICA-version and id already set
;; need an update method too, to fix these
(deftype Publisher [peer-config src-peer-id dst-task-id slot-id site ^Aeron
                    conn ^Publication publication status-mon
                    ^:unsynchronized-mutable short-id ^:unsynchronized-mutable replica-version 
                    ^:unsynchronized-mutable epoch]
  pub/Publisher
  (info [this]
    (let [dst-channel (autil/channel (:address site) (:port site))] 
      (assert (= dst-channel (.channel publication)))
      {:rv replica-version
       :e epoch
       :src-peer-id src-peer-id
       :dst-task-id dst-task-id
       :short-id short-id
       :slot-id slot-id
       :site site
       :dst-channel dst-channel
       :ready? (pub/ready? this)
       :session-id (.sessionId publication) 
       :stream-id (.streamId publication)
       :pos (.position publication)}))
  (key [this]
    [src-peer-id dst-task-id slot-id site])
  (equiv-meta [this pub-info]
    (and (= src-peer-id (:src-peer-id pub-info))
         (= dst-task-id (:dst-task-id pub-info))
         (= slot-id (:slot-id pub-info))
         (= site (:site pub-info))))
  (short-id [this] short-id)
  (set-short-id! [this new-short-id]
    (set! short-id new-short-id)
    this)
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (set! replica-version new-replica-version)
    (endpoint-status/set-replica-version! status-mon new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (assert new-epoch)
    (set! epoch new-epoch)
    this)
  (set-endpoint-peers! [this expected-peers]
    (endpoint-status/set-endpoint-peers! status-mon expected-peers)
    this)
  (start [this]
    (let [creator (.getStackTrace (Thread/currentThread))
          error-handler (reify ErrorHandler
                          (onError [this x] 
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging publication error" (clojure.string/join "\n" creator) x)
                            (taoensso.timbre/warn "Aeron messaging publication error:" creator x)))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          stream-id (onyx.messaging.aeron.utils/stream-id dst-task-id slot-id site)
          conn (Aeron/connect ctx)
          channel (autil/channel (:address site) (:port site))
          pub (.addPublication conn channel stream-id)
          status-mon (endpoint-status/start (new-endpoint-status peer-config src-peer-id (.sessionId pub)))]
      (Publisher. peer-config src-peer-id dst-task-id slot-id site conn
                  pub status-mon short-id replica-version epoch))) 
  (stop [this]
    (info "Stopping publisher" (.sessionId publication) [src-peer-id dst-task-id slot-id site])
    (when status-mon (endpoint-status/stop status-mon))
    (try
     (when publication (.close publication))
     (catch io.aeron.exceptions.RegistrationException re
       (info "Registration exception stopping publisher:" re)))
    (when conn (.close conn))
    (Publisher. peer-config src-peer-id dst-task-id slot-id site nil nil nil nil nil nil))
  (endpoint-status [this]
    status-mon)
  (ready? [this]
    (endpoint-status/ready? status-mon))
  (timed-out-subscribers [this]
    (endpoint-status/timed-out-subscribers status-mon))
  ; (alive? [this]
  ;   (and (.isConnected publication)
  ;        (endpoint-status/ready? status-mon)
  ;        (every? true? (vals (endpoint-status/liveness status-mon)))))
  (offer-ready! [this]
    (let [ready (ready replica-version src-peer-id short-id)
          payload ^bytes (messaging-compress ready)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication publication buf 0 (.capacity buf))]
      (debug "Offered ready message:" [ret ready :session-id (.sessionId publication) :site site])
      ret))
  (offer-heartbeat! [this]
    (let [msg (heartbeat replica-version epoch src-peer-id :any (.sessionId publication) short-id)
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication publication buf 0 (.capacity buf))] 
      (info "Pub offer heartbeat" (autil/channel (:address site) (:port site)) ret msg)
      ret))
  (poll-heartbeats! [this]
    (endpoint-status/poll! status-mon)
    this)
  (offer! [this buf endpoint-epoch]
    ;; TODO, remove the need to poll before every offer
    (endpoint-status/poll! status-mon)
    (cond (not (endpoint-status/ready? status-mon))
          (do
           ;; Send another ready message. 
           ;; Previous messages may have been sent before conn was fully established
           (pub/offer-ready! this)
           ;; Return not ready error code for now
           NOT_READY)

          (>= (endpoint-status/min-endpoint-epoch status-mon) endpoint-epoch)
          (.offer ^Publication publication ^UnsafeBuffer buf 0 (.capacity ^UnsafeBuffer buf))

          :else
          ENDPOINT_BEHIND)))

(defn new-publisher 
  [peer-config {:keys [src-peer-id dst-task-id slot-id site short-id] :as pub-info}]
  (->Publisher peer-config src-peer-id dst-task-id slot-id site nil nil nil short-id nil nil))

(defn reconcile-pub [peer-config publisher pub-info]
  (if-let [pub (cond (and publisher (nil? pub-info))
                     (do (pub/stop publisher)
                         nil)
                     (and (nil? publisher) pub-info)
                     (pub/start (new-publisher peer-config pub-info))
                     :else
                     publisher)]
    (-> pub 
        (pub/set-endpoint-peers! (:dst-peer-ids pub-info))
        (pub/set-short-id! (:short-id pub-info)))))
