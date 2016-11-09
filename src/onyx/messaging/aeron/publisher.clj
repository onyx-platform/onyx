(ns onyx.messaging.aeron.publisher
  (:require [onyx.messaging.aeron.utils :refer [action->kw]]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.endpoint-status :refer [new-endpoint-status]]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.types :refer [->Ready ->Heartbeat]]
            [onyx.messaging.messenger :as m]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [io.aeron Aeron Aeron$Context Publication]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]))

(def NOT_READY -55)

;; Need last heartbeat check time so we don't have to check everything too frequently?

(deftype Publisher 
  [messenger messenger-group job-id src-peer-id dst-task-id slot-id site stream-id 
   ;; Maybe these should be final and setup in the new-fn
   ^:unsynchronized-mutable ^Aeron conn 
   ^:unsynchronized-mutable ^Publication publication 
   ^:unsynchronized-mutable hb-mon]
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
    (endpoint-status/set-replica-version! hb-mon new-replica-version)
    this)
  (set-heartbeat-peers! [this expected-peers]
    (endpoint-status/set-heartbeat-peers! hb-mon expected-peers)
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
          conn* (Aeron/connect ctx)
          channel (common/aeron-channel (:address site) (:port site))
          pub (.addPublication conn* channel stream-id)
          hb-mon* (endpoint-status/start (new-endpoint-status messenger-group (.sessionId pub)))]
      ;; Create heartbeat channel here
      ;; We also need to send the heartbeats this way too
      (set! conn conn*)
      (set! publication pub)
      (set! hb-mon hb-mon*)
      (println "New pub:" (pub/pub-info this))
      this))
  (stop [this]
    ;; TODO SAFE STOP
    (endpoint-status/stop hb-mon)
    (.close publication)
    (.close conn)
    this)
  (ready? [this]
    (endpoint-status/ready? hb-mon))
  (offer-ready! [this]
    (let [ready (->Ready (m/replica-version messenger) src-peer-id dst-task-id)
          payload ^bytes (messaging-compress ready)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)
          ret (.offer ^Publication publication buf 0 (.capacity buf))]
      (println "Offered ready message:" ret)
      ret))
  (offer-heartbeat! [this]
    (let [msg (->Heartbeat (m/replica-version messenger) (m/id messenger) (.session-id publication))
          payload ^bytes (messaging-compress msg)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      (.offer ^Publication publication buf 0 (.capacity buf))))
  (poll-heartbeats! [this]
    (endpoint-status/poll! hb-mon)
    this)
  (offer! [this buf]
    ;; Split into different step?
    (if (endpoint-status/ready? hb-mon)
      (.offer ^Publication publication buf 0 (.capacity buf)) 
      (let [_ (endpoint-status/poll! hb-mon)]
        (pub/offer-ready! this)
        ;; Return not ready error code for now
        NOT_READY))))

(defn new-publisher [messenger messenger-group {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (->Publisher messenger messenger-group job-id src-peer-id dst-task-id slot-id site 
               (onyx.messaging.aeron.utils/stream-id job-id dst-task-id slot-id site)
               nil nil nil))

(defn reconcile-pub [messenger messenger-group publisher pub-info]
  (if-let [pub (cond (and publisher (nil? pub-info))
                     (do (pub/stop publisher)
                         nil)
                     (and (nil? publisher) pub-info)
                     (pub/start (new-publisher messenger messenger-group pub-info))
                     :else
                     publisher)]
    (-> pub 
        (pub/set-replica-version! (m/replica-version messenger))
        (pub/set-heartbeat-peers! (:dst-peer-ids pub-info)))))


