(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.types :refer [->Ready ->Heartbeat ->BarrierAlignedDownstream]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [taoensso.timbre :refer [info warn] :as timbre])
  (:import [io.aeron Aeron Aeron$Context Publication UnavailableImageHandler AvailableImageHandler]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]))

(def NOT_READY -55)
(def ENDPOINT_BEHIND -56) 

;; Need last heartbeat check time so we don't have to check everything too frequently?
(deftype Publisher [peer-config peer-id src-peer-id dst-task-id slot-id site ^Aeron conn ^Publication publication status-mon ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch]
  pub/Publisher
  (info [this]
    (let [dst-channel (autil/channel (:address site) (:port site))] 
      (assert (= dst-channel (.channel publication)))
      {:rv replica-version
       :e epoch
       :src-peer-id src-peer-id
       :dst-task-id dst-task-id
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
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (set! replica-version new-replica-version)
    (endpoint-status/set-replica-version! status-mon new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (assert new-epoch)
    (set! epoch new-epoch)
    (endpoint-status/set-epoch! status-mon new-epoch)
    this)
  (set-endpoint-peers! [this expected-peers]
    (endpoint-status/set-endpoint-peers! status-mon expected-peers)
    this)
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging publication error" x)
                            (taoensso.timbre/warn "Aeron messaging publication error:" x)))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          stream-id (onyx.messaging.aeron.utils/stream-id dst-task-id slot-id site)
          conn (Aeron/connect ctx)
          channel (autil/channel (:address site) (:port site))
          pub (.addPublication conn channel stream-id)
          status-mon (endpoint-status/start (new-endpoint-status peer-config peer-id (.sessionId pub)))]
      (Publisher. peer-config peer-id src-peer-id dst-task-id slot-id site conn
                  pub status-mon replica-version epoch))) 
  (stop [this]
    (info "Stopping publisher" [src-peer-id dst-task-id slot-id site])
    (when status-mon (endpoint-status/stop status-mon))
    (when publication (.close publication))
    (when conn (.close conn))
    (Publisher. peer-config peer-id src-peer-id dst-task-id slot-id site nil nil nil nil nil))
  (ready? [this]
    (endpoint-status/ready? status-mon))
  (alive? [this]
    (and (.isConnected publication)
         (endpoint-status/ready? status-mon)
         (every? true? (vals (endpoint-status/liveness status-mon)))))
  (offer-ready! [this]
    (let [ready (->Ready replica-version src-peer-id dst-task-id)
          payload ^bytes (messaging-compress ready)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication publication buf 0 (.capacity buf))]
      (info "Offered ready message:" [ret ready :session-id (.sessionId publication) :site site])
      ret))
  (offer-heartbeat! [this]
    (let [msg (->Heartbeat replica-version peer-id (.sessionId publication))
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      (.offer ^Publication publication buf 0 (.capacity buf))))
  (poll-heartbeats! [this]
    (endpoint-status/poll! status-mon)
    this)
  (offer! [this buf endpoint-epoch]
    ;; FIXME, need to poll endpoint status occasionally to get alive? reading
    ;; FIXME: Remove this poll and put it into task lifecycle maybe?
    ;; Poll all publishers in each iteration?
    (endpoint-status/poll! status-mon)

    ;; If we block offers because things are too far behind, we should allow barrier to be sent on a higher epoch
    ;;  but not a message on a higher epoch

    (info "Endpoint epoch vs vs" (endpoint-status/min-endpoint-epoch status-mon) endpoint-epoch)
    ;; Split into different step?
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
          ;; REPLACE WITH BACKPRESSURE CODE
          ENDPOINT_BEHIND)))

(defn new-publisher 
  [peer-config peer-id {:keys [src-peer-id dst-task-id slot-id site] :as pub-info}]
  (->Publisher peer-config peer-id src-peer-id dst-task-id slot-id site nil nil nil nil nil))

(defn reconcile-pub [peer-config peer-id publisher pub-info]
  (if-let [pub (cond (and publisher (nil? pub-info))
                     (do (pub/stop publisher)
                         nil)
                     (and (nil? publisher) pub-info)
                     (pub/start (new-publisher peer-config peer-id pub-info))
                     :else
                     publisher)]
    (pub/set-endpoint-peers! pub (:dst-peer-ids pub-info))))
