(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.aeron.utils :as autil
             :refer [action->kw stream-id heartbeat-stream-id try-close-publication try-close-conn]]
            [onyx.messaging.short-circuit :as sc]
            [onyx.messaging.serialize :as sz]
            [onyx.messaging.serializers.segment-encoder :as segment-encoder]
            [onyx.messaging.serializers.local-segment-encoder :as local-segment-encoder]
            [onyx.messaging.serializers.base-encoder :as base-encoder]
            [onyx.peer.constants :refer [NOT_READY ENDPOINT_BEHIND]]
            [onyx.static.default-vals :refer [arg-or-default]]
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

(defprotocol LocalRemoteDispatch
  (local-usage [this])
  (next-replica-version [this replica-version short-id session-id status-mon])
  (reset-segments-encoder [this])
  (encode-segment [this bs])
  (offer-segments [this publisher curr-position epoch]))

(deftype RemoteDispatch [^onyx.messaging.serializers.base_encoder.Encoder base-encoder
                         segments-encoder]
  LocalRemoteDispatch
  (next-replica-version [this replica-version short-id _ _] 
    (-> base-encoder
        (base-encoder/set-type onyx.types/message-id)
        (base-encoder/set-replica-version replica-version)
        (base-encoder/set-dest-id short-id))
    this)

  (local-usage [this]
    0)

  (reset-segments-encoder [this]
    (segment-encoder/wrap segments-encoder (base-encoder/length base-encoder)))

  (encode-segment [this bs]
    (if (segment-encoder/has-capacity? segments-encoder (alength ^bytes bs))
      (do (segment-encoder/add-message segments-encoder ^bytes bs)
          true)
      false))

  (offer-segments [this publisher curr-position epoch]
    (if (zero? (segment-encoder/segment-count segments-encoder))
      curr-position
      (let [segments-len (- (segment-encoder/offset segments-encoder)
                            (base-encoder/length base-encoder))] 
        (base-encoder/set-payload-length base-encoder segments-len)
        (pub/offer! publisher 
                    (.buffer base-encoder) 
                    (segment-encoder/offset segments-encoder) 
                    epoch)))))

(defrecord LocalDispatch [^onyx.messaging.serializers.base_encoder.Encoder base-encoder 
                          segments-encoder 
                          segments
                          batch-size
                          short-circuit-group
                          short-circuit
                          peer-config
                          job-id]
  LocalRemoteDispatch
  (next-replica-version [this replica-version short-id session-id status-mon]
    (let [buffer-size (* (arg-or-default :onyx.messaging/short-circuit-buffer-size peer-config)
                         (count (endpoint-status/statuses status-mon)))
          short-circuit (sc/get-init-short-circuit short-circuit-group job-id replica-version session-id buffer-size)]
      (-> base-encoder
          (base-encoder/set-type onyx.types/message-id)
          (base-encoder/set-replica-version replica-version)
          (base-encoder/set-dest-id short-id))
      (assoc this :short-circuit short-circuit)))

  (local-usage [this]
    {:batch-count-max (:max-batches short-circuit)
     :batch-count (sc/batch-count short-circuit)})

  (reset-segments-encoder [this]
    (vreset! segments (transient []))
    (local-segment-encoder/wrap segments-encoder
                                (base-encoder/length base-encoder)))

  (encode-segment [this segment]
    (if (< (count @segments) batch-size) 
      (do (conj! @segments segment)
          true)
      false))

  (offer-segments [this publisher curr-position epoch]
    (if (zero? (count @segments))
      curr-position
      ;; opportunistically place batch into short circuit buffer
      (if-let [long-ref (sc/add short-circuit @segments)]
        (let [_ (local-segment-encoder/add-batch-ref segments-encoder long-ref)
              ret (pub/offer! publisher 
                              (.buffer base-encoder) 
                              (+ (base-encoder/length base-encoder)
                                 (local-segment-encoder/length segments-encoder)) 
                              epoch)] 
          ;; offer failed, remove from short circuit buffer
          (when (neg? ret)
            (sc/get-and-remove short-circuit long-ref))
          ret)
        (Publication/BACK_PRESSURED)))))

(deftype Publisher
  [pub-info
   peer-config
   src-peer-id
   job-id
   dst-task-id
   slot-id
   site 
   serializer-fn
   ^UnsafeBuffer buffer
   ^UnsafeBuffer control-buffer 
   ^AtomicLong written-bytes
   ^AtomicLong errors
   ^Aeron conn 
   ^Publication publication 
   status-mon 
   error 
   ^:unsynchronized-mutable segment-sender
   ^:unsynchronized-mutable short-id
   ^:unsynchronized-mutable replica-version
   ^:unsynchronized-mutable epoch]
  pub/Publisher
  (info [this]
    (let [dst-channel (autil/channel (:address site) (:port site))] 
      {:rv replica-version
       :e epoch
       :job-id job-id
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
    (set! replica-version new-replica-version)
    (endpoint-status/set-replica-version! status-mon new-replica-version)
    (set! segment-sender 
          (next-replica-version segment-sender
                                replica-version 
                                short-id
                                (.sessionId publication) 
                                status-mon))
    this)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (serialize [this segment]
    (serializer-fn segment))
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
          stream-id (autil/stream-id dst-task-id slot-id site)
          conn (Aeron/connect ctx)
          channel (autil/channel (:address site) (:port site) (:term-buffer-size pub-info))
          pub (.addPublication conn channel stream-id)
          status-mon (endpoint-status/start (new-endpoint-status peer-config src-peer-id (.sessionId pub)))]
      (info "Starting publisher" channel)
      (Publisher. pub-info peer-config src-peer-id job-id dst-task-id slot-id site serializer-fn buffer 
                  control-buffer written-bytes errors conn pub status-mon error segment-sender 
                  short-id replica-version epoch))) 
  (stop [this]
    (info "Stopping publisher" (pub/info this))
    (some-> status-mon endpoint-status/stop)
    (some-> publication try-close-publication)
    (some-> conn try-close-conn)
    (Publisher. pub-info peer-config src-peer-id job-id dst-task-id slot-id site serializer-fn buffer
                control-buffer written-bytes errors nil nil nil error segment-sender nil nil nil))
  (endpoint-status [this]
    status-mon)
  (ready? [this]
    (endpoint-status/ready? status-mon))
  (statuses [this]
    (endpoint-status/statuses status-mon))
  (offer-ready! [this]
    (let [msg (ready replica-version src-peer-id short-id)
          len (sz/serialize control-buffer 0 msg)
          ret (.offer publication control-buffer 0 len)]
      (debug "Offered ready message:" [ret msg :session-id (.sessionId publication) :site site])
      ret))
  (metrics [this]
    {:short-circuit (local-usage segment-sender)
     :buffer-size-max (/ (.termBufferLength publication) 2)
     :position (.position publication)
     :buffer-ratio (float (/ (- (.positionLimit publication) (.position publication))
                                (/ (.termBufferLength publication) 2)))
     :buffer-unallocated (- (.positionLimit publication) (.position publication))})
  (reset-segment-encoder! [this]
    (reset-segments-encoder segment-sender))
  (encode-segment! [this payload]
    (encode-segment segment-sender payload))
  (offer-segments! [this]
    (offer-segments segment-sender this (.position publication) epoch))
  (offer-heartbeat! [this]
    (let [all-peers-uuid (java.util.UUID. 0 0)
          msg (heartbeat replica-version epoch src-peer-id all-peers-uuid (.sessionId publication) short-id)
          len (sz/serialize control-buffer 0 msg)
          ret (.offer publication control-buffer 0 len)] 
      (debug "Pub offer heartbeat" [ret (autil/channel (:address site) (:port site)) msg])
      ret))
  (poll-heartbeats! [this]
    (endpoint-status/poll! status-mon)
    this)
  (offer! [this buf length endpoint-epoch]
    (when @error (throw @error))
    ;; poll endpoints to try to unblock before offering
    (endpoint-status/poll! status-mon)
    (cond (not (endpoint-status/ready? status-mon))
          (do
           (pub/offer-ready! this)
           NOT_READY)

          (>= (endpoint-status/min-endpoint-epoch status-mon) endpoint-epoch)
          (let [ret (.offer ^Publication publication ^UnsafeBuffer buf 0 length)]
            (when (pos? ret) (.addAndGet written-bytes length))
            ret)

          :else
          ENDPOINT_BEHIND)))

(defn new-publisher 
  [{:keys [peer-config short-circuit] :as messenger-group} {:keys [written-bytes publication-errors] :as monitoring}
   {:keys [job-id src-peer-id dst-task-id slot-id site short-id 
           batch-size term-buffer-size short-circuit?] :as pub-info}]
  (let [max-message-length (long (/ term-buffer-size 8))
        buffer (UnsafeBuffer. (byte-array max-message-length))
        control-buffer (UnsafeBuffer. (byte-array onyx.types/max-control-message-size))
        base-encoder (base-encoder/->Encoder buffer 0)
        serializer-fn (if short-circuit? identity messaging-compress)
        segment-sender (if short-circuit? 
                         (->LocalDispatch base-encoder 
                                          (local-segment-encoder/->Encoder buffer nil)
                                          (volatile! nil)
                                          batch-size
                                          short-circuit
                                          nil
                                          peer-config
                                          job-id)
                         (->RemoteDispatch base-encoder 
                                           (segment-encoder/->Encoder buffer (long batch-size) nil nil)))]
    (->Publisher pub-info peer-config src-peer-id job-id dst-task-id slot-id site serializer-fn 
                 buffer control-buffer written-bytes publication-errors nil nil nil 
                 (atom nil) segment-sender short-id nil nil)))

(defn reconcile-pub [messenger-group monitoring publisher pub-info]
  (if-let [pub (cond (and publisher (nil? pub-info))
                     (do (pub/stop publisher)
                         nil)
                     (and (nil? publisher) pub-info)
                     (pub/start (new-publisher messenger-group monitoring pub-info))
                     :else
                     publisher)]
    (-> pub 
        (pub/set-endpoint-peers! (:dst-peer-ids pub-info))
        (pub/set-short-id! (:short-id pub-info)))))
