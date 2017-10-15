(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.aeron.status-publisher :refer [new-status-publisher]]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id 
                                                          try-close-conn try-close-subscription]]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.messaging.aeron.int2objectmap :refer [int2objectmap]]
            [onyx.messaging.aeron.utils :as autil]
            [onyx.messaging.short-circuit :as sc]
            [onyx.static.util :refer [ms->ns]]
            [onyx.messaging.serializers.segment-decoder :as sdec]
            [onyx.messaging.serializers.local-segment-decoder :as lsdec]
            [onyx.messaging.serializers.base-decoder :as bdec]
            [onyx.types :as t]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [onyx.messaging.aeron.int2objectmap CljInt2ObjectHashMap]
           [io.aeron Aeron Aeron$Context Publication Subscription Image
            ControlledFragmentAssembler UnavailableImageHandler
            AvailableImageHandler] 
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]))

(def fragment-limit-receiver 10000)

(defn counters->counter [ticket-counters job-id replica-version short-id session-id]
  (-> ticket-counters
      (get [job-id replica-version])
      (get short-id)
      (get session-id)))

(defn ^java.util.concurrent.atomic.AtomicLong lookup-ticket [ticket-counters job-id replica-version short-id session-id]
  (or (counters->counter @ticket-counters job-id replica-version short-id session-id)
      (-> ticket-counters
          (swap! update-in 
                 [[job-id replica-version] short-id session-id]
                 (fn [ticket]
                   (or ticket (AtomicLong. -1))))
          (counters->counter job-id replica-version short-id session-id))))

(defn assert-epoch-correct! [epoch message-epoch message]
  (when-not (= (inc epoch) message-epoch)
    (throw (ex-info "Unexpected barrier found. Possibly a misaligned subscription."
                    {:message message
                     :epoch epoch}))))

(defn invalid-replica-found! [replica-version message]
  (throw (ex-info "Shouldn't have received a message for this replica-version as we have not sent a ready message." 
                  {:replica-version replica-version 
                   :message message})))

(defn unavailable-image [sub-info]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (info "Unavailable network image" (.sessionId image) (.position image)
            :correlation-id (.correlationId image) sub-info))))

(defn available-image [sub-info lost-sessions]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (info "Available network image" (.sessionId image) (.position image)
            :correlation-id (.correlationId image) sub-info))))

(defn new-error-handler [error ^AtomicLong error-counter]
  (reify ErrorHandler
    (onError [this x] 
      (reset! error x)
      (.addAndGet error-counter 1))))

(defn check-correlation-id-alignment [aligned ^io.aeron.logbuffer.Header header]
  (when-not (get aligned (.correlationId ^Image (.context header)))
    (throw (ex-info "Lost and regained image with the same session-id and different correlation-id."
                    {:correlation-id (.correlationId ^Image (.context header))}))))

(deftype Subscriber 
  [peer-id
   ticket-counters
   peer-config
   job-id
   dst-task-id
   slot-id
   site
   batch-size
   short-circuit
   ^AtomicLong read-bytes 
   ^AtomicLong error-counter
   error
   ^bytes bs 
   channel
   ^Aeron conn
   ^Subscription subscription
   ^:unsynchronized-mutable aligned
   ^:unsynchronized-mutable sources
   ^:unsynchronized-mutable short-id-status-pub
   ^:unsynchronized-mutable status-pubs
   ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler 
   ^:unsynchronized-mutable replica-version
   ^:unsynchronized-mutable epoch
   ^:unsynchronized-mutable status
   ^:unsynchronized-mutable batch]
  sub/Subscriber
  (start [this]
    (let [media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          sinfo [dst-task-id slot-id site]
          aligned {}
          ctx (cond-> (.errorHandler (Aeron$Context.)
                                     (new-error-handler error error-counter))
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          stream-id (stream-id dst-task-id slot-id site)
          available-image-handler (available-image sinfo error)
          unavailable-image-handler (unavailable-image sinfo)
          sub (.addSubscription conn channel stream-id available-image-handler unavailable-image-handler)
          sources []
          short-id-status-pub (int2objectmap)
          status-pubs {}
          status {}
          sess-id->ticket {}
          new-subscriber (sub/add-assembler 
                          (Subscriber. peer-id ticket-counters peer-config job-id dst-task-id
                                       slot-id site batch-size short-circuit read-bytes error-counter error bs channel
                                       conn sub aligned sources short-id-status-pub 
                                       status-pubs nil nil nil status nil))]
      (info "Created subscriber" (sub/info new-subscriber))
      new-subscriber)) 
  (stop [this]
    (info "Stopping subscriber" [dst-task-id slot-id site] :registration-id (.registrationId subscription))
    (some-> subscription try-close-subscription)
    (some-> conn try-close-conn)
    (run! (comp status-pub/stop val) status-pubs)
    (Subscriber. peer-id ticket-counters peer-config job-id dst-task-id slot-id site
                 batch-size short-circuit read-bytes error-counter error bs nil nil nil
                 nil nil nil nil nil nil nil nil nil)) 
  (add-assembler [this]
    (set! assembler (ControlledFragmentAssembler. this))
    this)
  (info [this]
    {:subscription {:rv replica-version
                    :e epoch
                    :sources sources 
                    :dst-task-id dst-task-id 
                    :slot-id slot-id 
                    :blocked? (sub/blocked? this)
                    :site site
                    :channel (autil/channel peer-config)
                    :channel-id (.channel subscription)
                    :registration-id (.registrationId subscription)
                    :stream-id (.streamId subscription)
                    :closed? (.isClosed subscription)
                    :images (mapv autil/image->map (.images subscription))}
     :status-pubs (into {} (map (fn [[k v]] [k (status-pub/info v)]) status-pubs))})
  (equiv-meta [this sub-info]
    (and (= dst-task-id (:dst-task-id sub-info))
         (= slot-id (:slot-id sub-info))
         (= site (:site sub-info))))
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (set-replica-version! [this new-replica-version]
    (run! (comp status-pub/new-replica-version! val) status-pubs)
    (set! replica-version new-replica-version)
    (set! aligned #{})
    (set! status {})
    this)
  (recovered? [this]
    (:recovered? status))
  (get-recover [this]
    (:recover status))
  (unblock! [this]
    (run! (comp status-pub/unblock! val) status-pubs)
    this)
  (watermarks [this]
    (apply merge-with min (map status-pub/watermarks (vals status-pubs))))
  (blocked? [this]
    (not (some (complement status-pub/blocked?) (vals status-pubs))))
  (completed? [this]
    (not (some (complement status-pub/completed?) (vals status-pubs))))
  (checkpoint? [this]
    (not (some (complement status-pub/checkpoint?) (vals status-pubs))))
  (received-barrier! [this header barrier]
    (when-let [status-pub (get short-id-status-pub (:short-id barrier))]
      (assert-epoch-correct! epoch (:epoch barrier) barrier)
      (status-pub/block! status-pub)
      (status-pub/set-checkpoint! status-pub (:checkpoint? barrier))
      (status-pub/set-watermarks! status-pub (:watermarks barrier))
      (when (contains? barrier :completed?) 
        (status-pub/set-completed! status-pub (:completed? barrier)))
      (when (contains? barrier :recover-coordinates)
        (set! aligned (conj aligned (.correlationId ^Image (.context ^io.aeron.logbuffer.Header header))))
        (let [recover (:recover status)
              recover* (:recover-coordinates barrier)] 
          (when-not (or (nil? recover) (= recover* recover)) 
            (throw (ex-info "Two different subscribers sent differing recovery information."
                            {:recover1 recover :recover2 recover*
                             :replica-version replica-version :epoch epoch})))
          (set! status (assoc status :recover recover* :recovered? true)))))
    this)
  (poll! [this]
    (when @error (throw @error))
    (set! batch nil)
    (->> (.controlledPoll ^Subscription subscription
                          ^ControlledFragmentHandler assembler
                          fragment-limit-receiver)
         (.addAndGet read-bytes))
    batch)
  (offer-barrier-status! [this peer-id opts]
    (let [status-pub (get status-pubs peer-id)
          ret (status-pub/offer-barrier-status! status-pub replica-version epoch opts)] 
      (debug "Offer barrier status:" replica-version epoch ret)
      ret))
  (src-peers [this]
    (keys status-pubs))
  (status-pubs [this] status-pubs)
  (update-sources! [this sources*]
    (let [prev-peer-ids (set (keys status-pubs))
          next-peer-ids (set (map :src-peer-id sources*))
          peer-id->site (into {} (map (juxt :src-peer-id :site) sources*))
          rm-peer-ids (clojure.set/difference prev-peer-ids next-peer-ids)
          add-peer-ids (clojure.set/difference next-peer-ids prev-peer-ids)
          removed (reduce (fn [spubs src-peer-id]
                            (status-pub/stop (get spubs src-peer-id))
                            (dissoc spubs src-peer-id))
                          status-pubs
                          rm-peer-ids)
          final (reduce (fn [spubs src-peer-id]
                          (assoc spubs 
                                 src-peer-id
                                 (->> (get peer-id->site src-peer-id)
                                      (new-status-publisher peer-config peer-id src-peer-id)
                                      (status-pub/start))))
                        removed
                        add-peer-ids)
          short-id->status-pub (reduce (fn [m {:keys [short-id src-peer-id]}]
                                         (assoc m short-id (get final src-peer-id)))
                                       (int2objectmap)
                                       sources*)]
      (run! (fn [[short-id spub]] 
              (status-pub/set-short-id! spub short-id)) 
            short-id->status-pub)
      (set! short-id-status-pub short-id->status-pub)
      (set! status-pubs final)
      (set! sources sources*))
    this)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [base-dec (bdec/->Decoder buffer offset)
          msg-type (bdec/get-type base-dec)
          rv-msg (bdec/get-replica-version base-dec)
          short-id (bdec/get-dest-id base-dec)]
      (if (< rv-msg replica-version)
        ControlledFragmentHandler$Action/CONTINUE
        (if (> rv-msg replica-version)
          ControlledFragmentHandler$Action/ABORT
          (let [session-id (.sessionId header)
                spub (.valAt ^CljInt2ObjectHashMap short-id-status-pub short-id)]
            (some-> spub status-pub/set-heartbeat!)
            (cond (= msg-type t/message-id)
                  (do (when (nil? batch) (set! batch (transient [])))
                      (check-correlation-id-alignment aligned header)
                      (if (< (count batch) batch-size)
                        (let [ticket ^AtomicLong (status-pub/get-ticket spub)
                              ticket-val ^long (.get ticket)
                              position (.position header)
                              ticket? (and (< ticket-val position)
                                           (.compareAndSet ticket ticket-val position))]
                          (when ticket? 
                            (.addAndGet read-bytes length)
                            (if-let [s-circuit (status-pub/get-short-circuit spub)]
                              (->> (lsdec/wrap buffer (+ offset (bdec/length base-dec)))
                                   (lsdec/get-batch-ref)
                                   (sc/get-and-remove s-circuit)
                                   (persistent!)
                                   (reduce conj! batch))
                              (-> buffer
                                  (sdec/wrap bs (+ offset (bdec/length base-dec)))
                                  (sdec/read-segments! batch messaging-decompress))))
                          ControlledFragmentHandler$Action/CONTINUE)

                        ;; we've read a full batch worth
                        ControlledFragmentHandler$Action/ABORT))

                  (nil? spub)
                  ControlledFragmentHandler$Action/CONTINUE

                  (= msg-type t/heartbeat-id)
                  ;; all message types heartbeat above
                  ControlledFragmentHandler$Action/CONTINUE

                  (= msg-type t/barrier-id)
                  (if (nil? batch)
                    (do (when (pos? epoch) (check-correlation-id-alignment aligned header))
                        (sub/received-barrier! this header (sz/deserialize buffer offset))
                        ControlledFragmentHandler$Action/BREAK)
                    ControlledFragmentHandler$Action/ABORT)

                  (= msg-type t/ready-id)
                  ;; upstream says they're ready, so we perform all of the initialization required
                  ;; before receive messages
                  (let [smap (sc/get-short-circuit short-circuit job-id replica-version session-id)
                        ticket (lookup-ticket ticket-counters job-id replica-version short-id session-id)]
                    (-> spub
                        (status-pub/set-session-id! session-id ticket smap)
                        (status-pub/offer-ready-reply! replica-version epoch))
                    ControlledFragmentHandler$Action/CONTINUE)

                  :else
                  (throw (ex-info "Handler should never be here."
                                  {:replica-version replica-version
                                   :epoch epoch
                                   :message-type msg-type})))))))))

(defn new-subscription 
  [{:keys [peer-config short-circuit ticket-counters] :as messenger-group}
   {:keys [read-bytes subscription-errors] :as monitoring}
   peer-id
   sub-info]
  (let [{:keys [dst-task-id job-id slot-id site batch-size]} sub-info]
    (assert job-id)
    (->Subscriber peer-id ticket-counters peer-config job-id dst-task-id slot-id 
                  site batch-size short-circuit read-bytes subscription-errors (atom nil)
                  (byte-array (autil/max-message-length)) nil
                  nil nil nil nil nil nil nil nil nil nil nil)))
