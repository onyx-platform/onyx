(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.aeron.utils :as autil
             :refer [action->kw stream-id heartbeat-stream-id max-message-length]]
            [onyx.messaging.serialize :as sz]
            [onyx.messaging.serializers.segment-encoder :as segment-encoder]
            [onyx.messaging.serializers.base-encoder :as base-encoder]
            [onyx.peer.constants :refer [NOT_READY ENDPOINT_BEHIND]]
            [onyx.types :refer [heartbeat ready]]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [io.aeron Aeron Aeron$Context Publication UnavailableImageHandler AvailableImageHandler]
           [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]))

(defn assert-not-closed [ret]
  (if (= ret (Publication/CLOSED))
    (throw (Exception. "Offered to a closed publication. Rebooting."))))

(deftype Publisher [peer-config src-peer-id dst-task-id slot-id site 
                    ^UnsafeBuffer buffer ^UnsafeBuffer control-buffer base-encoder segment-encoder
                    ^AtomicLong written-bytes ^AtomicLong errors ^Aeron conn 
                    ^Publication publication status-mon error 
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
    (-> base-encoder
        (base-encoder/set-type onyx.types/message-id)
        (base-encoder/set-replica-version new-replica-version)
        (base-encoder/set-dest-id short-id))
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
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (.addAndGet errors 1)
                            (reset! error x)))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          stream-id (onyx.messaging.aeron.utils/stream-id dst-task-id slot-id site)
          conn (Aeron/connect ctx)
          channel (autil/channel (:address site) (:port site))
          pub (.addPublication conn channel stream-id)
          _ (when-not (= (.maxMessageLength pub) (max-message-length))
              (throw (ex-info (format "Max message payload differs between Aeron media driver and client.
                                       Ensure java property %s is equivalent between media driver and onyx peer." 
                                      autil/term-buffer-prop-name)
                              {:media-driver/max-length (max-message-length)
                               :publication/max-length (.maxMessageLength pub)})))
          status-mon (endpoint-status/start (new-endpoint-status peer-config src-peer-id (.sessionId pub)))]
      (Publisher. peer-config src-peer-id dst-task-id slot-id site buffer control-buffer base-encoder 
                  segment-encoder written-bytes errors conn pub status-mon error short-id replica-version epoch))) 
  (stop [this]
    (info "Stopping publisher" (pub/info this))
    (when status-mon (endpoint-status/stop status-mon))
    (try
     (when publication (.close publication))
     (catch io.aeron.exceptions.RegistrationException re
       (info "Registration exception stopping publisher:" re)))
    (when conn (.close conn))
    (Publisher. peer-config src-peer-id dst-task-id slot-id site buffer control-buffer 
                base-encoder segment-encoder written-bytes errors nil nil nil error nil nil nil))
  (endpoint-status [this]
    status-mon)
  (ready? [this]
    (endpoint-status/ready? status-mon))
  (statuses [this]
    (endpoint-status/statuses status-mon))
  (offer-ready! [this]
    (let [msg (ready replica-version src-peer-id short-id)
          len (sz/serialize control-buffer 0 msg)
          ret (.offer ^Publication publication control-buffer 0 len)]
      (assert-not-closed ret)
      (debug "Offered ready message:" [ret msg :session-id (.sessionId publication) :site site])
      ret))
  (segment-encoder [this]
    segment-encoder)
  (base-encoder [this]
    base-encoder)
  (offer-heartbeat! [this]
    (let [all-peers-uuid (java.util.UUID. 0 0)
          msg (heartbeat replica-version epoch src-peer-id all-peers-uuid (.sessionId publication) short-id)
          len (sz/serialize control-buffer 0 msg)
          ret (.offer ^Publication publication control-buffer 0 len)] 
      (debug "Pub offer heartbeat" (autil/channel (:address site) (:port site)) ret msg)
      (assert-not-closed ret)
      ret))
  (poll-heartbeats! [this]
    (endpoint-status/poll! status-mon)
    this)
  (offer! [this buf length endpoint-epoch]
    (when @error (throw @error))
    ;; TODO, remove the need to poll before every offer
    (endpoint-status/poll! status-mon)
    (cond (not (endpoint-status/ready? status-mon))
          (do
           (pub/offer-ready! this)
           NOT_READY)

          (>= (endpoint-status/min-endpoint-epoch status-mon) endpoint-epoch)
          (let [ret (.offer ^Publication publication ^UnsafeBuffer buf 0 length)]
            (if (pos? ret) 
              (.addAndGet written-bytes length)
              (assert-not-closed ret))
            ret)

          :else
          ENDPOINT_BEHIND)))

(defn new-publisher 
  [peer-config {:keys [written-bytes publication-errors] :as monitoring}
   {:keys [src-peer-id dst-task-id slot-id site short-id] :as pub-info}]
  (let [bs (byte-array (max-message-length)) 
        buffer (UnsafeBuffer. bs)
        control-buffer (UnsafeBuffer. (byte-array onyx.types/max-control-message-size))
        base-encoder (base-encoder/->Encoder buffer 0)
        segments-encoder (segment-encoder/->Encoder buffer nil nil)]
    (->Publisher peer-config src-peer-id dst-task-id slot-id site buffer control-buffer 
                 base-encoder segments-encoder written-bytes publication-errors nil nil 
                 nil (atom nil) short-id nil nil)))

(defn reconcile-pub [peer-config monitoring publisher pub-info]
  (if-let [pub (cond (and publisher (nil? pub-info))
                     (do (pub/stop publisher)
                         nil)
                     (and (nil? publisher) pub-info)
                     (pub/start (new-publisher monitoring peer-config pub-info))
                     :else
                     publisher)]
    (-> pub 
        (pub/set-endpoint-peers! (:dst-peer-ids pub-info))
        (pub/set-short-id! (:short-id pub-info)))))
