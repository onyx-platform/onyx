(ns onyx.messaging.aeron.subscriber
  (:require [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.status-publisher :as status-pub]
            [onyx.messaging.aeron.status-publisher :refer [new-status-publisher]]
            [onyx.messaging.common :as common]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [ms->ns]]
            [onyx.types]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [java.util.concurrent.atomic AtomicLong]
           [org.agrona.concurrent UnsafeBuffer]
           [org.agrona ErrorHandler]
           [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]
           [io.aeron Aeron Aeron$Context Publication Subscription Image
            ControlledFragmentAssembler UnavailableImageHandler
            AvailableImageHandler] 
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]))

;; FIXME to be tuned
(def fragment-limit-receiver 10000)

(defn ^java.util.concurrent.atomic.AtomicLong lookup-ticket [ticket-counters replica-version short-id session-id]
  (-> ticket-counters
      (swap! update-in 
             [replica-version short-id session-id]
             (fn [ticket]
               (or ticket (AtomicLong. -1))))
      (get-in [replica-version short-id session-id])))

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
      (info "Lost sessions now" @lost-sessions sub-info)
      (info "Unavailable network image" (.position image) (.sessionId image) sub-info))))

(defn available-image [sub-info]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (info "Available network image" (.position image) (.sessionId image) sub-info))))

(deftype Subscriber 
  [peer-id ticket-counters peer-config dst-task-id slot-id site batch-size ^bytes bs
   channel ^Aeron conn ^Subscription subscription lost-sessions 
   ^:unsynchronized-mutable sources         ^:unsynchronized-mutable short-id-status-pub
   ^:unsynchronized-mutable status-pubs     ^:unsynchronized-mutable ^ControlledFragmentAssembler assembler 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch
   ^:unsynchronized-mutable status          ^:unsynchronized-mutable batch
   ;; Going to need to be ticket per source, session-id per source, etc
   ;^:unsynchronized-mutable ^AtomicLong ticket 
   ]
  sub/Subscriber
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (println "Aeron messaging subscriber error" x)
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (taoensso.timbre/warn x "Aeron messaging subscriber error")))
          media-driver-dir (:onyx.messaging.aeron/media-driver-dir peer-config)
          sinfo [dst-task-id slot-id site]
          lost-sessions (atom #{})
          ctx (cond-> (Aeron$Context.)
                error-handler (.errorHandler error-handler)
                media-driver-dir (.aeronDirectoryName ^String media-driver-dir)
                true (.availableImageHandler (available-image sinfo))
                true (.unavailableImageHandler (unavailable-image sinfo lost-sessions)))
          conn (Aeron/connect ctx)
          channel (autil/channel peer-config)
          stream-id (stream-id dst-task-id slot-id site)
          sub (.addSubscription conn channel stream-id)]
      (info "Created subscriber" [dst-task-id slot-id site] :subscription (.registrationId sub))
      (sub/add-assembler 
       (Subscriber. peer-id ticket-counters peer-config dst-task-id
                    slot-id site batch-size bs channel
                    conn sub lost-sessions [] {} {} nil nil nil {} nil)))) 
  (stop [this]
    (info "Stopping subscriber" [dst-task-id slot-id site] :subscription (.registrationId subscription))
    ;; Can trigger this with really short timeouts
    ;; ^[[1;31mio.aeron.exceptions.RegistrationException^[[m: ^[[3mUnknown subscription link: 78^[[m
    ;; ^[[1;31mjava.util.concurrent.ExecutionException^[[m: ^[[3mio.aeron.exceptions.RegistrationException: Unknown subscription link: 78^[[m
    (when subscription 
      (try
       (.close subscription)
       (catch io.aeron.exceptions.RegistrationException re
         (info "Error stopping subscriber's subscription." re))))
    (when conn (.close conn))
    (run! status-pub/stop (vals status-pubs))
    (Subscriber. peer-id ticket-counters peer-config dst-task-id slot-id site
                 batch-size bs nil nil nil nil nil nil nil nil nil nil nil nil)) 
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
    ;; cache
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
    (debug "Before poll on channel" (sub/info this))
    (set! batch nil)
    (let [rcv (.controlledPoll ^Subscription subscription
                               ^ControlledFragmentHandler assembler
                               fragment-limit-receiver)]
      (debug "After poll" (sub/info this))
      batch))
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
          short-id->status-pub (->> sources*
                                    (map (fn [{:keys [short-id src-peer-id]}]
                                           [short-id (get final src-peer-id)]))
                                    (into {}))]
      (run! (fn [[short-id spub]] 
              (status-pub/set-short-id! spub short-id)) 
            short-id->status-pub)
      (set! short-id-status-pub short-id->status-pub)
      (set! status-pubs final)
      (set! sources sources*))
    this)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [msg-type (sz/get-message-type buffer offset)]
      (if (= msg-type sz/message-id)
        ;; handle batch of messages
        (let [decoder (sz/wrap-message-decoder buffer (inc offset))
              rv-msg (.replicaVersion decoder)
              short-id (.destId decoder)
              ;_ (println "Reading at offset" offset rv-msg short-id decoder)
              ;; FIXME, need faster int2objectmap here
              _ (when-let [spub (get short-id-status-pub short-id)]
                  (status-pub/set-heartbeat! spub))
              ;; set batch to a new transient here to minimise cost of poll when no
              ;; data is available in network publication images
              _ (when (nil? batch) (set! batch (transient [])))
              ret (if (= rv-msg replica-version)
                    (if (< (count batch) batch-size)
                      (let [session-id (.sessionId header)
                            ;; TODO: slow
                            ticket (lookup-ticket ticket-counters replica-version short-id session-id) 
                            ticket-val ^long (.get ticket)
                            position (.position header)
                            got-ticket? (and (< ticket-val position)
                                             (.compareAndSet ticket ticket-val position))]
                        (when got-ticket? (sz/into-segments! decoder bs batch))
                        ControlledFragmentHandler$Action/CONTINUE)

                      ;; we've read a batch worth
                      ControlledFragmentHandler$Action/ABORT) 

                    (if (< rv-msg replica-version)
                      ControlledFragmentHandler$Action/CONTINUE
                      ControlledFragmentHandler$Action/ABORT))]
          (debug [:read-subscriber (action->kw ret) channel dst-task-id])
          ret)

        ;; handle all other message types -- nasty code for now.
        (let [message (sz/deserialize buffer (inc offset) (dec length))
              rv-msg (:replica-version message)
              ret (if (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE
                    (if (> rv-msg replica-version)
                      ;; TODO, set marker here, so we don't boot off peers that are
                      ;; sending us messages but when we are behind the cluster
                      ;; We cannot update the heartbeat because we do not know the
                      ;; short-id mapping yet
                      ControlledFragmentHandler$Action/ABORT
                      (if-let [spub (get short-id-status-pub (:short-id message))]
                        (do (status-pub/set-heartbeat! spub)
                            (case (int (:type message))
                              1 (if (nil? batch)
                                  (do (sub/received-barrier! this message)
                                      ControlledFragmentHandler$Action/BREAK)
                                  ControlledFragmentHandler$Action/ABORT)
                              2 ControlledFragmentHandler$Action/CONTINUE
                              3 (do ;; FIXME: way too many ready messages are currently sent
                                    (-> spub
                                        (status-pub/set-session-id! (.sessionId header))
                                        (status-pub/offer-ready-reply! replica-version epoch))
                                    ControlledFragmentHandler$Action/CONTINUE)
                              (throw (ex-info "Handler should never be here."
                                              {:replica-version replica-version
                                               :epoch epoch
                                               :message message}))))
                        ControlledFragmentHandler$Action/CONTINUE)))]
          (debug [:read-subscriber (action->kw ret) channel dst-task-id] (into {} message))
          ret)))))

(defn new-subscription [peer-config monitoring peer-id ticket-counters sub-info]
  (let [{:keys [dst-task-id slot-id site batch-size]} sub-info]
    (->Subscriber peer-id ticket-counters peer-config dst-task-id slot-id site batch-size
                  ;; FIXME, make buffer size configurable
                  (byte-array 1000000) nil nil nil nil nil nil nil nil nil nil
                  nil nil)))
