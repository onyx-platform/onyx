(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [onyx.messaging.common :as mc]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message ->Barrier]]
            [onyx.messaging.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.subscriber-monitor :as sub-mon]
            [onyx.messaging.protocols.handler :as handler]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context ControlledFragmentAssembler Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler FragmentAssembler]
           [io.aeron.logbuffer FragmentHandler]
           [io.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]
           [org.agrona ErrorHandler]
           [org.agrona.concurrent 
            UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

;; TODO:
;; Use java.util.concurrent.atomic.AtomicLong for tickets

(defn barrier? [v]
  (instance? onyx.types.Barrier v))

(defn message? [v]
  (instance? onyx.types.Message v))

(defn hash-sub [sub-info]
  (hash (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site])))

;; FIXME to be tuned
(def fragment-limit-receiver 100)

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 100
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 1000
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10)
                                                (.toNanos TimeUnit/MICROSECONDS 1000))))


(defn get-threading-model
  [media-driver]
  (cond (= media-driver :dedicated) ThreadingMode/DEDICATED
        (= media-driver :shared) ThreadingMode/SHARED
        (= media-driver :shared-network) ThreadingMode/SHARED_NETWORK))

(defn stream-id [job-id task-id slot-id site src-peer-id]
  (hash [job-id task-id slot-id site #_src-peer-id]))

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  (println "GET PEER SITE" (mc/external-addr peer-config))
  {:address (mc/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

(defn delete-aeron-directory-safe [^MediaDriver$Context media-driver-context]
  (try (.deleteAeronDirectory media-driver-context)
       (catch java.nio.file.NoSuchFileException nsfe
         (info "Couldn't delete aeron media dir. May have been already deleted by shutdown hook." nsfe))))

(def aeron-dir-name (atom nil))
;(reset! aeron-dir-name (str "/var/folders/d8/6x6y27ln2f702g56jzzh9h780000gn/T/aeron-lucas" (java.util.UUID/randomUUID)))

(defrecord EmbeddedMediaDriver [peer-config]
  component/Lifecycle
  (start [component]
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? peer-config)
          threading-mode (get-threading-model (arg-or-default :onyx.messaging.aeron/embedded-media-driver-threading peer-config))
          media-driver-context (if embedded-driver?
                                 (do
                                  ;(reset! aeron-dir-name (str "/var/folders/d8/6x6y27ln2f702g56jzzh9h780000gn/T/aeron-lucas-" (java.util.UUID/randomUUID)))
                                  (-> (MediaDriver$Context.) 
                                      (.threadingMode threading-mode)
                                      ;(.aeronDirectoryName @aeron-dir-name)
                                      (.dirsDeleteOnStart true))))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))]
      (when embedded-driver? 
        (.addShutdownHook (Runtime/getRuntime) 
                          (Thread. (partial delete-aeron-directory-safe media-driver-context))))
      (when embedded-driver? (println "MEDIADRIVER:" @aeron-dir-name))
      (assoc component 
             :media-driver media-driver 
             :media-driver-context media-driver-context)))
  (stop [{:keys [media-driver media-driver-context subscribers] :as component}]
    (when media-driver 
      (.close ^MediaDriver media-driver))
    #_(when media-driver-context 
      (delete-aeron-directory-safe media-driver-context))
    (assoc component :media-driver nil :media-driver-context nil)))

(defrecord AeronMessagingPeerGroup [peer-config]
  component/Lifecycle
  (start [component]
    (println "Start aeron")
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [bind-addr (common/bind-addr peer-config)
          external-addr (common/external-addr peer-config)
          port (:onyx.messaging/peer-port peer-config)
          ticket-counters (atom {})
          embedded-media-driver (component/start (->EmbeddedMediaDriver peer-config))]
      (assoc component
             :bind-addr bind-addr
             :external-addr external-addr
             :ticket-counters ticket-counters
             :embedded-media-driver embedded-media-driver
             :port port)))

  (stop [{:keys [embedded-media-driver] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (component/stop embedded-media-driver)
    (assoc component :embedded-media-driver nil :bind-addr nil 
           :external-addr nil :external-channel nil :ticket-counters nil)))

(defmethod m/build-messenger-group :aeron [peer-config]
  (map->AeronMessagingPeerGroup {:peer-config peer-config}))

(defn subscription-aligned?
  [sub-ticket]
  (empty? (:aligned sub-ticket)))

; (defn is-next-barrier? [replica-version epoch barrier]
;   (and (= replica-version (:replica-version barrier))
;        (= (inc epoch) (:epoch barrier))))

; (defn found-next-barrier? [messenger subscriber]
;   (let [barrier-val @(sub/get-barrier subscriber)] 
;     (println "Is next barrier" (m/replica-version messenger) (m/epoch messenger) barrier-val "emitted?" (:emitted? barrier-val))
;     (and (is-next-barrier? (m/replica-version messenger) (m/epoch messenger) barrier-val) 
;          (not (:emitted? barrier-val)))))

; (defn unblocked? [messenger barrier]
;   (let [barrier-val @barrier] 
;     (and (= (m/replica-version messenger) (:replica-version barrier-val))
;          (= (m/epoch messenger) (:epoch barrier-val))
;          (:emitted? barrier-val))))

(defn action->kw [action]
  (cond (= action ControlledFragmentHandler$Action/CONTINUE)
        :CONTINUE
        (= action ControlledFragmentHandler$Action/BREAK)
        :BREAK
        (= action ControlledFragmentHandler$Action/ABORT)
        :ABORT
        (= action ControlledFragmentHandler$Action/COMMIT)
        :COMMIT))
  
(deftype RecoverFragmentHandler 
  [src-peer-id dst-task-id ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable recover]
  handler/PartialSubscriber
  (set-replica-version! [this new-replica-version]
    (assert (or (nil? replica-version) (> new-replica-version replica-version)))
    (set! replica-version new-replica-version)
    (set! recover nil)
    this)
  (init [this]
    this)
  (get-recover [this] 
    recover)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          rv-msg (:replica-version message)
          _ (println "rv-msg" rv-msg " rv" replica-version)
          ret (cond (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (and (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id))
                         (= rv-msg replica-version))
                    ControlledFragmentHandler$Action/CONTINUE

                    
                    (and (barrier? message)
                         (and (= replica-version (:replica-version message))
                              (= 1 (:epoch message))))
                    (do 
                     (assert (:recover message))
                     (set! recover (:recover message))
                     ControlledFragmentHandler$Action/BREAK)

                    :else
                    ControlledFragmentHandler$Action/ABORT)]
      (println [:recover (action->kw ret) dst-task-id src-peer-id] (.position header) message)
      ;(add-tracked-message! messenger dst-task-id src-peer-id message ret [:poll-new-barrier dst-task-id src-peer-id])
      ret)))

;; TODO, can skip everything if it's the wrong image completely
(deftype ReadSegmentsFragmentHandler 
  [src-peer-id dst-task-id ^:unsynchronized-mutable blocked ^:unsynchronized-mutable ticket ^:unsynchronized-mutable batch 
   ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable epoch ^:unsynchronized-mutable completed]
  handler/PartialSubscriber
  (set-replica-version! [this new-replica-version]
    (assert (or (nil? replica-version) (> new-replica-version replica-version)))
    (set! replica-version new-replica-version)
    this)
  (set-epoch! [this new-epoch]
    (set! epoch new-epoch)
    this)
  (init [this new-ticket]
    (set! ticket new-ticket)
    (set! batch (transient []))
    this)
  (get-batch [this]
    (persistent! batch))
  (blocked? [this] 
    blocked)
  (unblock! [this]
    (set! blocked false)
    this)
  (block! [this]
    (set! blocked false)
    this)
  (completed? [this]
    completed)
  ControlledFragmentHandler
  (onFragment [this buffer offset length header]
    ;(println "ON FRAGMENT " ticket)
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          ;; FIXME, why 2?
          n-desired-messages 2
          ticket-val @ticket
          position (.position header)
          rv-msg (:replica-version message)
          ret (cond (and (or (not= (:dst-task-id message) dst-task-id)
                             (not= (:src-peer-id message) src-peer-id))
                         (= rv-msg replica-version))
                    ControlledFragmentHandler$Action/CONTINUE

                    (< rv-msg replica-version)
                    ControlledFragmentHandler$Action/CONTINUE

                    (> rv-msg replica-version)
                    ControlledFragmentHandler$Action/ABORT

                    (>= (count batch) n-desired-messages)
                    ControlledFragmentHandler$Action/ABORT

                    (and (message? message)
                         (< ticket-val position))
                    (do 
                     (assert (= replica-version rv-msg))
                     ;; FIXME, not sure if this logically works.
                     ;; If ticket gets updated in mean time, then is this always invalid and should be continued?
                     ;; WORK OUT ON PAPER
                     (when (compare-and-set! ticket ticket-val position)
                       (do 
                        (assert (coll? (:payload message)))
                        (reduce conj! batch (:payload message))))
                     ControlledFragmentHandler$Action/CONTINUE)

                    (and (barrier? message)
                         (= replica-version (:replica-version message)))
                    (do
                     (when-not (= (inc epoch) (:epoch message))
                       (throw (Exception. "Unexpected barrier found. Possibly a misaligned subscription.")))
                     ;(println "Got barrier " message)
                     (if (zero? (count batch)) ;; empty? broken on transients
                       (do 
                        (set! blocked true)
                        ;; For use determining whether job is complete. Refactor later
                        (when (:completed? message)
                          (set! completed true))
                        ControlledFragmentHandler$Action/BREAK)  
                       ControlledFragmentHandler$Action/ABORT))

                    :else
                    (throw (Exception. (str "Should not happen? " ticket-val " " position " " replica-version " " epoch))))]
      (println [:handle-message (action->kw ret) dst-task-id src-peer-id] (.position header) message)
      ;(add-tracked-message! messenger dst-task-id src-peer-id message ret [:handle-message dst-task-id src-peer-id])
      ret)))

(defn handle-drain
  [sub-info buffer offset length header]
  (let [ba (byte-array length)
        _ (.getBytes ^UnsafeBuffer buffer offset ba)
        message (messaging-decompress ba)]
    ;(println "DRAIN MESSAGE SUB hash" (hash-sub sub-info) sub-info "message" message)
    ControlledFragmentHandler$Action/CONTINUE))

(defn unavailable-image-drainer [sub-info]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (.println (System/out) (str "UNAVAILABLE image " (.position image) " " (.sessionId image) " " sub-info)))))

(defn available-image [sub-info]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (.println (System/out) (str "AVAILABLE image " (.position image) " " (.sessionId image) " " sub-info)))))

(deftype Subscriber 
  [messenger messenger-group job-id src-peer-id dst-task-id slot-id site 
   stream-id 
   ;; Maybe these should be final and setup in the new-fn
   ^:unsynchronized-mutable ^Aeron conn 
   ^:unsynchronized-mutable ^Subscription subscription 
   ^:unsynchronized-mutable recover-handler
   ^:unsynchronized-mutable recover-assembler
   ^:unsynchronized-mutable segments-handler
   ^:unsynchronized-mutable segments-assembler
   ;^:unsynchronized-mutable heartbeat-monitor
   ]
  sub/Subscriber
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            (println "Aeron messaging subscriber error" x)
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (taoensso.timbre/warn x "Aeron messaging subscriber error")))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler)
                  ;; make these do stuff to whether it's ready or not?
                  #_(.availableImageHandler (available-image sub-info))
                  #_(.unavailableImageHandler (unavailable-image-drainer sub-info)))
          conn* (Aeron/connect ctx)
          bind-addr (:bind-addr messenger-group)
          port (:port messenger-group)
          channel (mc/aeron-channel bind-addr port)
          sub (.addSubscription conn* channel stream-id)
          ;; Get rid of barrier atom and just track what replica and epoch that you've seen and whether you're blocked or
          ;; not. Maybe just whether you're blocked or not
          ;; Will also need a recover thing
          recover-fragment-handler (->RecoverFragmentHandler src-peer-id dst-task-id nil nil)
          recover-fragment-assembler (ControlledFragmentAssembler. recover-fragment-handler)
          segments-fragment-handler (->ReadSegmentsFragmentHandler src-peer-id dst-task-id nil nil nil nil nil nil)
          segments-fragment-assembler (ControlledFragmentAssembler. segments-fragment-handler)]
      (set! recover-handler recover-fragment-handler)
      (set! recover-assembler recover-fragment-assembler)
      (set! segments-handler segments-fragment-handler)
      (set! segments-assembler segments-fragment-assembler)
      ;; Create heartbeat channel here
      ;; We also need to send the heartbeats this way too
      (set! conn conn*)
      (set! subscription sub)
      (println "New sub:" (sub/sub-info this))
      this))
  (stop [this]
    ;(sub-mon/stop heartbeat-monitor)
    (.close subscription)
    (.close conn)
    this)
  (key [this]
    [src-peer-id dst-task-id slot-id site])
  (sub-info [this]
    [:rv (m/replica-version messenger)
     :e (m/epoch messenger)
     :blocked? (sub/blocked? this)
     :channel-id (.channel subscription)
     :registation-id (.registrationId subscription)
     :stream-id (.streamId subscription)
     :closed? (.isClosed subscription)
     :images (mapv (fn [i] [:pos (.position i) 
                            :term-id (.initialTermId i) 
                            :session-id (.sessionId i) 
                            :closed? (.isClosed i) 
                            :corr-id (.correlationId i) 
                            :source-id (.sourceIdentity i)]) 
                   (.images subscription)) 
     :sub-info {:src-peer-id src-peer-id 
                :dst-task-id dst-task-id 
                :slot-id slot-id 
                :site site}])
  (equiv-meta [this sub-info]
    #_(println "EQUIV?" src-peer-id (:src-peer-id sub-info)
         dst-task-id (:dst-task-id sub-info)
         slot-id (:slot-id sub-info)
         site (:site sub-info))


    (and (= src-peer-id (:src-peer-id sub-info))
         (= dst-task-id (:dst-task-id sub-info))
         (= slot-id (:slot-id sub-info))
         (= site (:site sub-info))))
  (set-epoch! [this new-epoch]
    (handler/set-epoch! segments-handler new-epoch)
    this)
  (set-replica-version! [this new-replica-version]
    (handler/set-replica-version! segments-handler new-replica-version)
    (handler/set-replica-version! recover-handler new-replica-version)
    (handler/block! segments-handler)
    this)
  (get-recover [this]
    (handler/get-recover recover-handler))
  (unblock! [this]
    (handler/unblock! segments-handler)
    this)
  (blocked? [this]
    (handler/blocked? segments-handler))
  (completed? [this]
    (handler/completed? segments-handler))
  (poll-messages! [this]
    (if-not (sub/blocked? this)
      (let [_ (handler/init segments-handler 
                            ;; TICKET
                            (atom -1))
            _ (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler segments-assembler fragment-limit-receiver)
            batch (handler/get-batch segments-handler)]
        (println "polled batch:" batch)
        batch)
      []))
  (poll-replica! [this]
    (handler/init recover-handler)
    (.controlledPoll ^Subscription subscription ^ControlledFragmentHandler recover-assembler fragment-limit-receiver)))

;(deftype Subscriber [subscription stream conn recover-handler recover-assembler segments-handler segments])

(defn new-subscription 
  [messenger messenger-group {:keys [job-id src-peer-id dst-task-id slot-id site] :as sub-info}]
  (->Subscriber messenger messenger-group job-id src-peer-id dst-task-id slot-id site 
                (stream-id job-id dst-task-id slot-id site src-peer-id)
                nil nil nil nil nil nil))

;; Peer heartbeats look like
; {:last #inst "2016-10-29T07:31:52.303-00:00"
;  :replica-version 42
;  :peer #uuid "75ec283f-2202-4c7a-b98f-d6fba42e486f"}

(def heartbeat-stream-id 0)

(deftype HeartBeatMonitor 
  [messenger-group ^:unsynchronized-mutable conn ^:unsynchronized-mutable subscription 
   session-id ^:unsynchronized-mutable replica-version ^:unsynchronized-mutable peers ^:unsynchronized-mutable ready-peers
   ^:unsynchronized-mutable heartbeats ^:unsynchronized-mutable ready]
  sub-mon/SubscriberMonitor
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging heartbeat error" x)
                            (taoensso.timbre/warn "Aeron messaging heartbeat error:" x)))
          ctx (-> (Aeron$Context.)
                  (.errorHandler error-handler))
          conn* (Aeron/connect ctx)
          channel (mc/aeron-channel (:bind-addr messenger-group) (:port messenger-group))
          sub (.addSubscription conn* channel heartbeat-stream-id)]
      (set! ready false)
      (set! heartbeats {})
      (set! ready-peers [])
      (set! conn conn*)
      (set! subscription sub)
      this))
  (stop [this]
    (.close subscription)
    (.close conn)
    this)
  (poll! [this]
    (.poll ^Subscription subscription ^FragmentHandler this fragment-limit-receiver))
  (ready? [this]
    ready)
  (set-replica-version! [this new-peers new-replica-version]
    (set! ready false)
    (set! replica-version new-replica-version)
    (set! peers new-peers)
    (set! heartbeats {})
    this)
  FragmentHandler
  (onFragment [this buffer offset length header]
    (let [ba (byte-array length)
          _ (.getBytes ^UnsafeBuffer buffer offset ba)
          message (messaging-decompress ba)
          msg-rv (:replica-version message)
          msg-sess (:session-id message)]
      (when (and (= session-id msg-sess)
                 (= replica-version msg-rv))
        (case (:type message)
          :ready (let [peer-id (:peer-id message)
                       new-ready (conj ready-peers peer-id)] 
                   (println "PUB: ready-peer" peer-id "session-id" session-id)
                   (set! ready-peers new-ready)
                   (set! heartbeats (assoc heartbeats peer-id (System/currentTimeMillis)))
                   (when (= ready-peers peers)
                     (println "PUB: all peers ready.")
                     (set! ready true)))
          :heartbeat (let [peer-id (:peer-id message)] 
                       (println "PUB: peer heartbeat:" peer-id ". Time since last heartbeat:" 
                                (- (get heartbeats peer-id) 
                                   (System/currentTimeMillis)))
                       (set! heartbeats (assoc heartbeats peer-id (System/currentTimeMillis)))))
        ;(println [:recover (action->kw ret) dst-task-id src-peer-id] (.position header) message)
        ))))

(defn new-heartbeat-monitor [messenger-group session-id]
  (HeartBeatMonitor. messenger-group nil nil session-id nil nil nil nil false)) 

;; Need last heartbeat check time so we don't have to check everything too frequently?
(deftype Publisher 
  [messenger messenger-group job-id src-peer-id dst-task-id slot-id site 
   stream-id 
   ;; Maybe these should be final and setup in the new-fn
   ^:unsynchronized-mutable ^Aeron conn 
   ^:unsynchronized-mutable ^Publication publication 
   ^:unsynchronized-mutable heartbeat-monitor]
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
  ;(poll-heartbeats! [this])
  (start [this]
    (let [error-handler (reify ErrorHandler
                          (onError [this x] 
                            ;(System/exit 1)
                            ;; FIXME: Reboot peer
                            (println "Aeron messaging publication error" x)
                            (taoensso.timbre/warn "Aeron messaging publication error:" x)))
          ctx (-> (Aeron$Context.)
                  ;(.aeronDirectoryName @aeron-dir-name)
                  (.errorHandler error-handler))
          conn* (Aeron/connect ctx)
          channel (mc/aeron-channel (:address site) (:port site))
          pub (.addPublication conn* channel stream-id)
          hb-mon (sub-mon/start (new-heartbeat-monitor messenger-group (.sessionId pub)))]
      ;; Create heartbeat channel here
      ;; We also need to send the heartbeats this way too
      (set! conn conn*)
      (set! publication pub)
      (set! heartbeat-monitor hb-mon)
      (println "New pub:" (pub/pub-info this))
      this))
  (stop [this]
    (sub-mon/stop heartbeat-monitor)
    (.close publication)
    (.close conn)
    this)
  (offer! [this buf]
    (sub-mon/poll! heartbeat-monitor)
    (.offer ^Publication publication buf 0 (.capacity buf))))

(defn new-publication [messenger messenger-group {:keys [job-id src-peer-id dst-task-id slot-id site] :as pub-info}]
  (->Publisher messenger messenger-group job-id src-peer-id dst-task-id slot-id site 
               (stream-id job-id dst-task-id slot-id site src-peer-id)
               nil nil nil))

(defn add-to-subscriptions [subscriptions sub-info]
  (conj (or subscriptions []) sub-info))

(defn remove-from-subscriptions 
  [subscriptions sub-info]
  {:post [(= (dec (count subscriptions)) (count %))]}
  (let [to-remove (first (filter #(sub/equiv-meta % sub-info) subscriptions))] 
    (assert to-remove)
    (sub/stop to-remove)
    (vec (remove #(sub/equiv-meta % sub-info) subscriptions))))

(defn remove-from-publications [publications pub-info]
  {:pre [(pos? (count publications))]
   :post [(= (dec (count publications)) (count %))]}
  (println "Count publications" (count publications))
  (let [to-remove (first (filter #(pub/equiv-meta % pub-info) publications))] 
    (assert to-remove)
    (pub/stop to-remove)
    (vec (remove #(pub/equiv-meta % pub-info) publications))))

;; TICKETS SHOULD USE session id (unique publication) and position
;; Lookup task, then session id, then position, skip over positions that are lower, use ticket to take higher
;; Stick tickets in peer messenger group in single atom?
;; Have tickets be cleared up when image is no longer available?
;; Use these to manage tickets
;; onAvailableImage
;; onUnavailableImage

(defn flatten-publishers [publications]
  (reduce into [] (vals publications)))

(defn reconcile-sub [messenger messenger-group subscriber sub-info]
  (cond (and subscriber (nil? sub-info))
        (do (sub/stop subscriber)
            nil)
        (and (nil? subscriber) sub-info)
        (sub/start (new-subscription messenger messenger-group sub-info))
        :else
        subscriber))

(defn transition-subscriptions [messenger messenger-group subscribers sub-infos]
  (let [m-prev (into {} 
                     (map (juxt sub/key identity))
                     subscribers)
        m-next (into {} 
                     (map (juxt (juxt :src-peer-id :dst-task-id :slot-id :site) 
                                identity))
                     sub-infos)
        all-keys (into (set (keys m-prev)) 
                       (keys m-next))]
    (->> all-keys
         (keep (fn [k]
                 (let [old (m-prev k)
                       new (m-next k)]
                   (reconcile-sub messenger messenger-group old new))))
         (map (fn [sub]
                (sub/set-replica-version! sub (m/replica-version messenger))))
         (vec))))

(defn reconcile-pub [messenger messenger-group publisher pub-info]
  (cond (and publisher (nil? pub-info))
        (do (pub/stop publisher)
            nil)
        (and (nil? publisher) pub-info)
        (pub/start (new-publication messenger messenger-group pub-info))
        :else
        publisher))

(defn transition-publishers [messenger messenger-group publishers pub-infos]
  (println "Publishers" publishers)
  (let [m-prev (into {} 
                     (map (juxt pub/key identity))
                     (flatten-publishers publishers))
        m-next (into {} 
                     (map (juxt (juxt :src-peer-id :dst-task-id :slot-id :site) 
                                identity))
                     pub-infos)
        all-keys (into (set (keys m-prev)) 
                       (keys m-next))]
    (->> all-keys
         (keep (fn [k]
                 (let [old (m-prev k)
                       new (m-next k)]
                   (reconcile-pub messenger messenger-group old new))))
         (group-by (fn [pub]
                     [(.dst-task-id pub) (.slot-id pub)])))))


(deftype AeronMessenger [messenger-group 
                         id 
                         ^:unsynchronized-mutable ticket-counters 
                         ^:unsynchronized-mutable replica-version 
                         ^:unsynchronized-mutable epoch 
                         ^:unsynchronized-mutable publications 
                         ^:unsynchronized-mutable subscriptions 
                         ^:unsynchronized-mutable read-index]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    (run! pub/stop (flatten-publishers publications))
    (run! sub/stop subscriptions)
    (set! ticket-counters nil)
    (set! replica-version nil)
    (set! epoch nil)
    (set! publications nil)
    (set! subscriptions nil)
    component)

  m/Messenger
  (publications [messenger]
    (flatten-publishers publications))

  (subscriptions [messenger]
    subscriptions)

  (update-publishers [messenger pub-infos]
    (set! publications (transition-publishers messenger messenger-group publications pub-infos))
    messenger)

  (update-subscriptions [messenger sub-infos]
    (set! subscriptions (transition-subscriptions messenger messenger-group subscriptions sub-infos))
    messenger)

  (add-subscription [messenger sub-info]
    #_(set! subscriptions 
          (add-to-subscriptions subscriptions 
                                (sub/start (new-subscription messenger messenger-group sub-info))))
    messenger)

  (remove-subscription [messenger sub-info]
    #_(set! subscriptions 
          (remove-from-subscriptions subscriptions sub-info))
    messenger)

  (register-ticket [messenger sub-info]
    (assert (<= (count (:aligned-peers sub-info)) 1))
    ;; TODO, clear previous versions at some point? Have to worry about other threads though
    (swap! ticket-counters 
           update 
           replica-version 
           (fn [tickets]
             (update (or tickets {}) 
                     [(:src-peer-id sub-info)
                      (:dst-task-id sub-info)]
                     (fn [sub-ticket]
                       (if sub-ticket
                         ;; Already know what peers should be aligned
                         (update sub-ticket :aligned disj id)
                         {:ticket (atom -1)
                          :aligned (disj (set (:aligned-peers sub-info)) id)})))))
    (println "Registered ticket " @ticket-counters)
    messenger)

  (get-ticket [messenger {:keys [dst-task-id src-peer-id] :as sub}]
    (get-in @ticket-counters [replica-version [src-peer-id dst-task-id]]))

  ;; ADD OR UPDATE. NEEDS TO FIGURE OUT ALIGNED PEERS
  (add-publication [messenger pub-info]
    (println "Add publication:" pub-info)
    (set! publications 
          (update-in publications
                     [(:dst-task-id pub-info) (:slot-id pub-info)]
                     (fn [pbs] 
                       (assert (= id (:src-peer-id pub-info)) [id (:src-peer-id pub-info)])
                       (conj (or pbs []) 
                             (pub/start (new-publication messenger messenger-group pub-info))))))

    messenger)

  (remove-publication [messenger pub-info]
    (println "Remove publication:" pub-info)
    (set! publications (update-in publications 
                                  [(:dst-task-id pub-info) (:slot-id pub-info)] 
                                  remove-from-publications 
                                  pub-info))
    messenger)

  (set-replica-version! [messenger rv]
    (assert (or (nil? replica-version) (> rv replica-version)) [rv replica-version])
    (set! read-index 0)
    (set! replica-version rv)
    (m/set-epoch! messenger 0)
    (reduce m/register-ticket messenger subscriptions))

  (replica-version [messenger]
    replica-version)

  (epoch [messenger]
    epoch)

  (set-epoch! [messenger e]
    (assert (or (nil? epoch) (> e epoch) (zero? e)))
    (set! epoch e)
    (run! #(sub/set-epoch! % e) subscriptions)
    messenger)

  (next-epoch! [messenger]
    (m/set-epoch! messenger (inc epoch)))

  (poll [messenger]
    ;; TODO, poll all subscribers in one poll?
    ;; TODO, test for overflow?
    (let [subscriber (get subscriptions (mod read-index (count subscriptions)))
          messages (sub/poll-messages! subscriber)] 
      (set! read-index (inc read-index))
      (mapv t/input messages)))

  (offer-segments [messenger batch {:keys [dst-task-id slot-id] :as task-slot}]
    ;; Problem here is that if no slot will accept the message we will
    ;; end up having to recompress on the next offer
    ;; Possibly should try more than one iteration before returning
    ;; TODO: should re-use unsafe buffers in aeron messenger. 
    ;; Will require nippy to be able to write directly to unsafe buffers
    (let [message (->Message id dst-task-id slot-id (m/replica-version messenger) batch)
          payload ^bytes (messaging-compress message)
          buf ^UnsafeBuffer (UnsafeBuffer. payload)] 
      ;; shuffle publication order to ensure even coverage. FIXME: slow
      ;; FIXME, don't use SHUFFLE AS IT FCKS WITH REPRO. Also slow
      (println "Publications " publications "getting" [dst-task-id slot-id])
      (loop [pubs (shuffle (get publications [dst-task-id slot-id]))]
        (if-let [publisher (first pubs)]
          (let [ret (pub/offer! publisher buf)]
            (println "Offer segment" [:ret ret :message message :pub (pub/pub-info publisher)])
            (if (neg? ret)
              (recur (rest pubs))
              task-slot))))))

  (poll-recover [messenger]
    (loop [sbs subscriptions]
      (let [sub (first sbs)] 
        (when sub 
          (if-not (sub/get-recover sub)
            (sub/poll-replica! sub)
            (println "poll-recover!, has barrier, skip:" (sub/sub-info sub)))
          (recur (rest sbs)))))
    (debug "Seen all subs?: " (m/all-barriers-seen? messenger) :subscriptions (mapv sub/sub-info subscriptions))
    (if (empty? (remove sub/get-recover subscriptions))
      (let [recover (sub/get-recover (first subscriptions))] 
        (assert recover)
        (assert (= 1 (count (set (map sub/get-recover subscriptions)))) "All subscriptions should recover at same checkpoint.")
        recover)))

  (offer-barrier [messenger pub-info]
    (onyx.messaging.messenger/offer-barrier messenger pub-info {}))

  (offer-barrier [messenger publisher barrier-opts]
    (assert (.dst-task-id publisher))
    (let [barrier (merge (->Barrier id (.dst-task-id publisher) (m/replica-version messenger) (m/epoch messenger))
                         (assoc barrier-opts 
                                ;; Extra debug info
                                :site (.site publisher)
                                :stream (.stream-id publisher)
                                :new-id (java.util.UUID/randomUUID)))
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress barrier))]
      (let [ret (pub/offer! publisher buf)] 
        (println "Offer barrier:" [:ret ret :message barrier :pub (pub/pub-info publisher)])
        ret)))

  (unblock-subscriptions! [messenger]
    (run! sub/unblock! subscriptions)
    messenger)

  (all-barriers-seen? [messenger]
    (empty? (remove sub/blocked? subscriptions)))

  (all-barriers-completed? [messenger]
    (empty? (remove sub/completed? subscriptions))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (->AeronMessenger messenger-group id (:ticket-counters messenger-group) nil nil nil nil 0))

(defmethod clojure.core/print-method AeronMessagingPeerGroup
  [system ^java.io.Writer writer]
  (.write writer "#<Aeron Peer Group>"))
