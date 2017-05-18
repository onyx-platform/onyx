(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.aeron.utils :as autil
             :refer [action->kw stream-id heartbeat-stream-id max-message-length 
                     try-close-publication try-close-conn]]
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

(defn build-local-reset-encoder-fn
  [base-encoder local-segments-encoder segments]
  (fn []
    (vreset! segments (transient []))
    (local-segment-encoder/wrap local-segments-encoder
                                (base-encoder/length base-encoder))))

(defn build-remote-reset-encoder-fn [base-encoder segments-encoder]
    (fn []
      (segment-encoder/wrap segments-encoder
                            (base-encoder/length base-encoder))))

(defn build-local-encode-segment-fn [batch-size segments]
  (fn [segment]
    (if (< (count @segments) batch-size) 
      (do (conj! @segments segment)
          true)
      false)))

(defn build-remote-encode-segment-fn [segments-encoder]
  (fn [^bytes bs]
    (if (segment-encoder/has-capacity? segments-encoder (alength bs))
      (do (segment-encoder/add-message segments-encoder bs)
          true)
      false)))

(defn build-local-offer-segments-fn [publisher peer-config short-circuit status-mon 
                                     job-id replica-version session-id segments
                                     ^onyx.messaging.serializers.base_encoder.Encoder base-encoder
                                     local-segment-encoder]
  (let [;; pre-lookup short circuit map
        buffer-size (* (arg-or-default :onyx.messaging/short-circuit-buffer-size peer-config)
                       (count (endpoint-status/statuses status-mon)))
        sc (sc/get-init-short-circuit short-circuit job-id replica-version session-id buffer-size)] 
    (fn [^Publication pub epoch]
      (if (zero? (count @segments))
        (.position pub)
        (if-let [long-ref (sc/add sc @segments)]
          (let [_ (local-segment-encoder/add-batch-ref local-segment-encoder long-ref)
                ret (pub/offer! publisher 
                                (.buffer base-encoder) 
                                (+ (base-encoder/length base-encoder)
                                   (local-segment-encoder/length local-segment-encoder)) 
                                epoch)] 
            ; Lets be safe and remove it again since we didn't message it
            (when (neg? ret)
              (sc/get-and-remove sc long-ref))
            ret)
          (Publication/BACK_PRESSURED))))))

(defn build-remote-offer-segments-fn 
  [publisher ^onyx.messaging.serializers.base_encoder.Encoder base-encoder segment-encoder]
  (fn [^Publication pub epoch]
    (if (zero? (segment-encoder/segment-count segment-encoder))
      (.position pub)
      (let [segments-len (- (segment-encoder/offset segment-encoder)
                            (base-encoder/length base-encoder))] 
        (base-encoder/set-payload-length base-encoder segments-len)
        (pub/offer! publisher (.buffer base-encoder) (segment-encoder/offset segment-encoder) epoch)))))


(deftype Publisher
  [peer-config
   src-peer-id
   job-id
   dst-task-id
   slot-id
   site 
   serializer-fn
   ^UnsafeBuffer buffer
   ^UnsafeBuffer control-buffer 
   ^onyx.messaging.serializers.base_encoder.Encoder base-encoder
   ^onyx.messaging.serializers.segment_encoder.Encoder segment-encoder
   local-segment-encoder
   segments
   short-circuit?
   short-circuit
   ^AtomicLong written-bytes
   ^AtomicLong errors
   ^Aeron conn 
   ^Publication publication 
   status-mon 
   error 
   reset-segment-encoder-fn
   encode-segment-fn
   ^:unsynchronized-mutable offer-segments-fn
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
    (set! offer-segments-fn (if short-circuit? 
                              (build-local-offer-segments-fn
                               this peer-config short-circuit status-mon 
                               job-id replica-version (.sessionId publication) segments 
                               base-encoder local-segment-encoder)
                              (build-remote-offer-segments-fn this base-encoder segment-encoder)))
    (-> base-encoder
        (base-encoder/set-type onyx.types/message-id)
        (base-encoder/set-replica-version new-replica-version)
        (base-encoder/set-dest-id short-id))
    (endpoint-status/set-replica-version! status-mon new-replica-version)
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
          channel (autil/channel (:address site) (:port site))
          pub (.addPublication conn channel stream-id)
          status-mon (endpoint-status/start (new-endpoint-status peer-config src-peer-id (.sessionId pub)))]
      (when-not (= (.maxMessageLength pub) (max-message-length))
        (throw (ex-info (format "Max message payload differs between Aeron media driver and client.
                                 Ensure java property %s is equivalent between media driver and onyx peer." 
                                autil/term-buffer-prop-name)
                        {:media-driver/max-length (max-message-length)
                         :publication/max-length (.maxMessageLength pub)})))
      (info "Starting publisher" channel)
      (Publisher. peer-config src-peer-id job-id dst-task-id slot-id site serializer-fn
                  buffer control-buffer base-encoder segment-encoder local-segment-encoder segments 
                  short-circuit? short-circuit written-bytes errors conn pub status-mon error
                  reset-segment-encoder-fn encode-segment-fn offer-segments-fn short-id replica-version epoch))) 
  (stop [this]
    (info "Stopping publisher" (pub/info this))
    (some-> status-mon endpoint-status/stop)
    (some-> publication try-close-publication)
    (some-> conn try-close-conn)
    (Publisher. peer-config src-peer-id job-id dst-task-id slot-id site serializer-fn buffer
                control-buffer base-encoder segment-encoder local-segment-encoder segments 
                short-circuit? short-circuit written-bytes errors nil nil nil error
                reset-segment-encoder-fn encode-segment-fn offer-segments-fn nil nil nil))
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
  (reset-segment-encoder! [this]
    (reset-segment-encoder-fn))
  (encode-segment! [this payload]
    (encode-segment-fn payload))
  (offer-segments! [this]
    (offer-segments-fn publication epoch))
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
    ;; TODO, remove the need to poll before every offer
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
   {:keys [job-id src-peer-id dst-task-id slot-id site short-id batch-size] :as pub-info}]
  (let [buffer (UnsafeBuffer. (byte-array (max-message-length)))
        control-buffer (UnsafeBuffer. (byte-array onyx.types/max-control-message-size))
        base-encoder (base-encoder/->Encoder buffer 0)
        segments-encoder (segment-encoder/->Encoder buffer (long batch-size) nil nil)
        local-segments-encoder (local-segment-encoder/->Encoder buffer nil)
        short-circuit? (autil/short-circuit? peer-config site)
        serializer-fn (if short-circuit? identity messaging-compress)
        segments (volatile! nil)
        reset-encoder-fn (if short-circuit? 
                           (build-local-reset-encoder-fn base-encoder local-segments-encoder segments)
                           (build-remote-reset-encoder-fn base-encoder segments-encoder))
        encode-segment-fn (if short-circuit? 
                            (build-local-encode-segment-fn batch-size segments)
                            (build-remote-encode-segment-fn segments-encoder))]
    (->Publisher peer-config src-peer-id job-id dst-task-id slot-id site serializer-fn buffer control-buffer 
                 base-encoder segments-encoder local-segments-encoder segments short-circuit? 
                 short-circuit written-bytes publication-errors nil nil nil (atom nil) 
                 reset-encoder-fn encode-segment-fn nil short-id nil nil)))

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
