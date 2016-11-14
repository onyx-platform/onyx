(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.types :refer [->Ready ->Heartbeat ->BarrierAligned]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [io.aeron Aeron Aeron$Context Publication]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]))

(def NOT_READY -55)

;; Need last heartbeat check time so we don't have to check everything too frequently?
(deftype Publisher [peer-config messenger job-id src-peer-id dst-task-id slot-id site ^Aeron conn ^Publication publication status-mon]
  pub/Publisher
  (pub-info [this]
    [:rv (m/replica-version messenger)
     :e (m/epoch messenger)
     :session-id (.sessionId publication) 
     :stream-id (.streamId publication)
     :pos (.position publication)
     :pub-info {:src-peer-id src-peer-id
                :dst-task-id dst-task-id
                :slot-id slot-id
                :site site}])
  (key [this]
    [src-peer-id dst-task-id slot-id site])
  (equiv-meta [this pub-info]
    (and (= src-peer-id (:src-peer-id pub-info))
         (= dst-task-id (:dst-task-id pub-info))
         (= slot-id (:slot-id pub-info))
         (= site (:site pub-info))))
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (endpoint-status/set-replica-version! status-mon new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (assert new-epoch)
    (endpoint-status/set-epoch! status-mon new-epoch)
    this)
  (set-endpoint-peers! [this expected-peers]
    (endpoint-status/set-endpoint-peers! status-mon expected-peers)
    this)
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging publication error" x)
                            (taoensso.timbre/warn "Aeron messaging publication error:" x)))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler))
          stream-id (onyx.messaging.aeron.utils/stream-id job-id dst-task-id slot-id site)
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          pub (.addPublication conn channel stream-id)
          status-mon (endpoint-status/start (new-endpoint-status peer-config (.sessionId pub)))]
      (Publisher. peer-config messenger job-id src-peer-id dst-task-id slot-id site conn pub status-mon)))
  (stop [this]
    (when status-mon (endpoint-status/stop status-mon))
    (when publication (.close publication))
    (when conn (.close conn))
    (Publisher. peer-config messenger job-id src-peer-id dst-task-id slot-id site nil nil nil))
  (ready? [this]
    (endpoint-status/ready? status-mon))
  (alive? [this]
    (and (.isConnected publication)
         (endpoint-status/ready? status-mon)
         (every? true? (vals (endpoint-status/liveness status-mon)))))
  (offer-ready! [this]
    (let [ready (->Ready (m/replica-version messenger) src-peer-id dst-task-id)
          payload ^bytes (messaging-compress ready)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication publication buf 0 (.capacity buf))]
      (println "Offered ready message:" src-peer-id dst-task-id slot-id site ret)
      ret))
  
  (offer-heartbeat! [this]
    (let [msg (->Heartbeat (m/replica-version messenger) (m/id messenger) (.sessionId publication))
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      (.offer ^Publication publication buf 0 (.capacity buf))))
  (poll-heartbeats! [this]
    (endpoint-status/poll! status-mon)
    this)
  ;; FIXME, need to poll endpoint status occasionally to get alive? reading
  (offer! [this buf]
    ;; Split into different step?
    (if (endpoint-status/ready? status-mon)
      (.offer ^Publication publication buf 0 (.capacity buf)) 
      (do
       ;; Try to get new ready replies
       (endpoint-status/poll! status-mon)
       ;; Send another ready message. 
       ;; Previous messages may have been sent before conn was fully established
       (pub/offer-ready! this)
       ;; Return not ready error code for now
       NOT_READY))))

(defn new-publisher 
  [peer-config messenger {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (->Publisher peer-config messenger job-id src-peer-id dst-task-id slot-id site nil nil nil))

(defn reconcile-pub [peer-config messenger publisher pub-info]
  (if-let [pub (cond (and publisher (nil? pub-info))
                     (do (pub/stop publisher)
                         nil)
                     (and (nil? publisher) pub-info)
                     (pub/start (new-publisher peer-config messenger pub-info))
                     :else
                     publisher)]
    (pub/set-endpoint-peers! pub (:dst-peer-ids pub-info))))
