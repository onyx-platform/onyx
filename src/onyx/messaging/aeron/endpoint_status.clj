(ns onyx.messaging.aeron.endpoint-status
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.common :as common]
            [onyx.messaging.protocols.endpoint-status :as endpoint-status]
            [onyx.peer.constants :refer [initialize-epoch]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns]]
            [taoensso.timbre :refer [debug info warn trace] :as timbre])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent UnsafeBuffer]))

;; FIXME to be tuned
(def fragment-limit-receiver 1000)

(defn initial-statuses [peer-ids]
  (->> peer-ids
       (map (fn [p] 
              [p {:ready? false
                  :heartbeat (System/nanoTime)
                  :epoch initialize-epoch
                  :min-epoch initialize-epoch}]))
       (into {})))

(deftype EndpointStatus 
  [peer-config peer-id session-id liveness-timeout-ns ^Aeron conn ^Subscription subscription 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch 
   ^:unsynchronized-mutable statuses        ^:unsynchronized-mutable ready]
  onyx.messaging.protocols.endpoint-status/EndpointStatus
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging heartbeat error" x)
                            (taoensso.timbre/warn "Aeron messaging heartbeat error:" x)))
          media-driver-dir ^String (:onyx.messaging.aeron/media-driver-dir peer-config)
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName media-driver-dir))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          sub (.addSubscription conn channel heartbeat-stream-id)
          liveness-timeout-ns (ms->ns (arg-or-default :onyx.peer/subscriber-liveness-timeout-ms peer-config))]
      (info "Started endpoint status on peer:" peer-id)
      (EndpointStatus. peer-config peer-id session-id liveness-timeout-ns conn
                       sub replica-version epoch statuses ready)))
  (stop [this]
    (info "Stopping endpoint status" [peer-id])
    (try
     (.close subscription)
     (catch Throwable t
       (info "Error closing endpoint subscription:" t)))
     (.close conn)
    (EndpointStatus. peer-config peer-id session-id nil nil nil nil nil nil false))
  (info [this]
    [:rv replica-version
     :e epoch
     :channel-id (.channel subscription)
     :registration-id (.registrationId subscription)
     :stream-id (.streamId subscription)
     :closed? (.isClosed subscription)
     :images (mapv autil/image->map (.images subscription))
     :statuses statuses
     :ready? ready])
  (poll! [this]
    (.poll ^Subscription subscription ^FragmentHandler this fragment-limit-receiver))
  (set-endpoint-peers! [this expected-peers]
    (->> expected-peers
         (initial-statuses)
         (set! statuses))
    this)
  (ready? [this]
    ready)
  (timed-out-subscribers [this]
    (let [curr-time (System/nanoTime)]
      (sequence (comp (filter (fn [[peer-id status]] 
                                (< (+ (:heartbeat status) liveness-timeout-ns)
                                   curr-time)))
                      (map key))
                statuses)))
  (min-endpoint-epoch [this]
    (apply min (map :epoch (vals statuses))))
  (min-downstream-epoch [this]
    (apply min (map :min-epoch (vals statuses))))
  (set-replica-version! [this new-replica-version]
    (assert new-replica-version)
    (set! replica-version new-replica-version)
    (set! ready false)
    (->> (keys statuses)
         (initial-statuses)
         (set! statuses))
    this)
  FragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          msg-rv (:replica-version message)
          msg-sess (:session-id message)]
      (info "Statuses" statuses)
      (when (and (= session-id msg-sess) (= replica-version msg-rv))
        (info "MESSAGE" message)
        (case (int (:type message))
          2 (when (= peer-id (:dst-peer-id message))
              (let [src-peer-id (:src-peer-id message)
                    epoch (:epoch message)
                    prev-epoch (get-in statuses [src-peer-id :epoch])]
                (set! statuses (-> statuses
                                   (assoc-in [src-peer-id :replica-version] (:replica-version message))
                                   (assoc-in [src-peer-id :min-epoch] (:min-epoch message))
                                   (assoc-in [src-peer-id :heartbeat] (System/nanoTime))))
                (cond (= epoch (inc prev-epoch))
                      (set! statuses (assoc-in statuses [src-peer-id :epoch] epoch))

                      (= epoch prev-epoch)
                      :heartbeat

                      :else
                      (do
                       (info 
                        "Received epoch is not in sync with expected epoch." 
                                      {:our-replica-version replica-version
                                       :prev-epoch prev-epoch
                                       :epoch epoch
                                       :message message}
                        )
                      
                       (throw (ex-info "Received epoch is not in sync with expected epoch." 
                                      {:our-replica-version replica-version
                                       :prev-epoch prev-epoch
                                       :epoch epoch
                                       :message message}))))))

          4 (when (= peer-id (:dst-peer-id message))
              (let [src-peer-id (:src-peer-id message)] 
                (set! statuses (-> statuses
                                   (assoc-in [src-peer-id :ready?] true)
                                   (assoc-in [src-peer-id :heartbeat] (System/nanoTime))))
                ;; calculate whether we're fully ready, for performance use
                (set! ready (not (some false? (map (comp :ready? val) statuses))))))

          (throw (ex-info "Invalid message type" {:message message})))))))

(defn new-endpoint-status [peer-config peer-id session-id]
  (->EndpointStatus peer-config peer-id session-id nil nil nil nil nil nil false)) 
