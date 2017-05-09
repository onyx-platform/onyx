(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.aeron.status-publisher :refer [new-status-publisher]]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.messaging.aeron.int2objectmap :refer [int2objectmap]]
            [onyx.messaging.aeron.utils :as autil]
            [onyx.static.util :refer [ms->ns]]
            [onyx.messaging.serializers.segment-decoder :as sdec]
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

(defn counters->counter [ticket-counters replica-version short-id session-id]
  (-> ticket-counters
      (get replica-version)
      (get short-id)
      (get session-id)))

;; TODO, faster lookup via int2objectmap
(defn ^java.util.concurrent.atomic.AtomicLong lookup-ticket [ticket-counters replica-version short-id session-id]
  (or (counters->counter @ticket-counters replica-version short-id session-id)
      (-> ticket-counters
          (swap! update-in 
                 [replica-version short-id session-id]
                 (fn [ticket]
                   (or ticket (AtomicLong. -1))))
          (counters->counter replica-version short-id session-id))))

(defn assert-epoch-correct! [epoch message-epoch message]
  (when-not (= (inc epoch) message-epoch)
    (throw (ex-info "Unexpected barrier found. Possibly a misaligned subscription."
                    {:message message
                     :epoch epoch}))))

(defn invalid-replica-found! [replica-version message]
  (throw (ex-info "Shouldn't have received a message for this replica-version as we have not sent a ready message." 
                  {:replica-version replica-version 
                   :message message})))

(defn unavailable-image [sub-info lost-sessions]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (swap! lost-sessions conj (.sessionId image))
      (info "Unavailable network image" (.sessionId image) (.position image) :correlation-id (.correlationId image) sub-info)
      (debug "Lost sessions now" @lost-sessions sub-info))))

(defn available-image [sub-info lost-sessions]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (info "Available network image" (.position image) (.sessionId image) :correlation-id (.correlationId image) sub-info))))

(deftype Subscriber 
  [peer-id
   ticket-counters
   peer-config
   dst-task-id
   slot-id
   site
   batch-size
   ^AtomicLong read-bytes 
   ^AtomicLong errors
   error
   ^bytes bs 
   channel
   ^Aeron conn
   ^Subscription subscription
   lost-sessions
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
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (reset! error x)
                            (.addAndGet errors 1)))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          sinfo [dst-task-id slot-id site]
          lost-sessions (atom #{})
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir)
                true (.availableImageHandler (available-image sinfo error))
                true (.unavailableImageHandler (unavailable-image sinfo lost-sessions)))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          stream-id (stream-id dst-task-id slot-id site)
          sub (.addSubscription conn channel stream-id)
          sources []
          short-id-status-pub (int2objectmap)
          status-pubs {}
          status {}
          new-subscriber (sub/add-assembler 
                          (Subscriber. peer-id ticket-counters peer-config dst-task-id
                                       slot-id site batch-size read-bytes errors error bs channel
                                       conn sub lost-sessions sources short-id-status-pub 
                                       status-pubs nil nil nil status nil))]
      (info "Created subscriber" (sub/info new-subscriber))
      new-subscriber)) 
  (stop [this]
    (info "Stopping subscriber" [dst-task-id slot-id site] :registration-id (.registrationId subscription))
    (when subscription 
      (try
       (.close subscription)
       (catch io.aeron.exceptions.RegistrationException re
         (info "Error stopping subscriber's subscription." re))))
    (when conn (.close conn))
    (run! status-pub/stop (vals status-pubs))
    (Subscriber. peer-id ticket-counters peer-config dst-task-id slot-id site
                 batch-size read-bytes errors error bs nil nil nil nil nil nil nil 
                 nil nil nil nil nil)) 
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
    (run! status-pub/new-replica-version! (vals status-pubs))
    (set! replica-version new-replica-version)
    (set! status {})
    this)
  (recovered? [this]
    (:recovered? status))
  (get-recover [this]
    (:recover status))
  (unblock! [this]
    (run! (comp status-pub/unblock! val) status-pubs)
    this)
  (blocked? [this]
    ;; TODO: precompute result whenever a status changes
    (not (some (complement status-pub/blocked?) (vals status-pubs))))
  (completed? [this]
    (not (some (complement status-pub/completed?) (vals status-pubs))))
  (received-barrier! [this barrier]
    (when-let [status-pub (get short-id-status-pub (:short-id barrier))]
      (assert-epoch-correct! epoch (:epoch barrier) barrier)
      (status-pub/block! status-pub)
      (when (contains? barrier :completed?) 
        (status-pub/set-completed! status-pub (:completed? barrier)))
      (when (contains? barrier :recover-coordinates)
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
          (let [spub (.valAt ^CljInt2ObjectHashMap short-id-status-pub short-id)]
            (some-> spub status-pub/set-heartbeat!)
            (cond (= msg-type t/message-id)
                  (let [seg-dec (sdec/wrap buffer bs (+ offset (bdec/length base-dec)))]
                    (when (nil? batch) (set! batch (transient [])))
                    (if (< (count batch) batch-size)
                      (let [session-id (.sessionId header)
                            ;; FIXME: slow
                            ticket (lookup-ticket ticket-counters replica-version short-id session-id) 
                            ticket-val ^long (.get ticket)
                            position (.position header)
                            ticket? (and (< ticket-val position)
                                         (.compareAndSet ticket ticket-val position))]
                        (.addAndGet read-bytes length)
                        (when ticket? (sdec/read-segments! seg-dec batch messaging-decompress))
                        ControlledFragmentHandler$Action/CONTINUE)

                      ;; we've read a full batch worth
                      ControlledFragmentHandler$Action/ABORT))

                  (nil? spub)
                  ControlledFragmentHandler$Action/CONTINUE

                  (= msg-type t/heartbeat-id)
                  ;; already heartbeated above for all message types
                  ControlledFragmentHandler$Action/CONTINUE

                  (= msg-type t/barrier-id)
                  (if (nil? batch)
                    (do (sub/received-barrier! this (sz/deserialize buffer offset))
                        ControlledFragmentHandler$Action/BREAK)
                    ControlledFragmentHandler$Action/ABORT)

                  (= msg-type t/ready-id)
                  (do (-> spub
                          (status-pub/set-session-id! (.sessionId header))
                          (status-pub/offer-ready-reply! replica-version epoch))
                      ControlledFragmentHandler$Action/CONTINUE)

                  :else
                  (throw (ex-info "Handler should never be here."
                                  {:replica-version replica-version
                                   :epoch epoch
                                   :message-type msg-type})))))))))

(defn new-subscription [peer-config monitoring peer-id ticket-counters sub-info]
  (let [{:keys [dst-task-id slot-id site batch-size]} sub-info]
    (->Subscriber peer-id ticket-counters peer-config dst-task-id slot-id site batch-size
                  (:read-bytes monitoring) (:subscription-errors monitoring) (atom nil)
                  (byte-array (autil/max-message-length)) nil nil nil 
                  nil nil nil nil nil nil nil nil nil)))
