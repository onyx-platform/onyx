(ns ^:no-doc onyx.messaging.aeron.messenger
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message 
                                      ->Barrier ->Ready ->ReadyReply ->Heartbeat]]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.protocols.handler :as handler]
            [onyx.messaging.aeron.subscriber :refer [reconcile-sub]]
            [onyx.messaging.aeron.publisher :refer [reconcile-pub]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription Image 
            UnavailableImageHandler AvailableImageHandler]
           [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

;; TODO:
;; Use java.util.concurrent.atomic.AtomicLong for tickets
;(def al (java.util.concurrent.atomic.AtomicLong.)) 

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  {:address (common/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

(defn unavailable-image [sub-info]
  (reify UnavailableImageHandler
    (onUnavailableImage [this image] 
      (.println (System/out) (str "UNAVAILABLE image " (.position image) " " (.sessionId image) " " sub-info)))))

(defn available-image [sub-info]
  (reify AvailableImageHandler
    (onAvailableImage [this image] 
      (.println (System/out) (str "AVAILABLE image " (.position image) " " (.sessionId image) " " sub-info)))))

;; Peer heartbeats look like
; {:last #inst "2016-10-29T07:31:52.303-00:00"
;  :replica-version 42
;  :peer #uuid "75ec283f-2202-4c7a-b98f-d6fba42e486f"}


;; TICKETS SHOULD USE session id (unique publication) and position
;; Lookup task, then session id, then position, skip over positions that are lower, use ticket to take higher
;; Stick tickets in peer messenger group in single atom?
;; Have tickets be cleared up when image is no longer available?
;; Use these to manage tickets
;; onAvailableImage
;; onUnavailableImage

(defn flatten-publishers [publishers]
  (reduce into [] (vals publishers)))

(defn transition-subscribers [messenger messenger-group subscribers sub-infos]
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
         (vec))))

(defn transition-publishers [messenger messenger-group publishers pub-infos]
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
                         ticket-counters 
                         ^:unsynchronized-mutable replica-version 
                         ^:unsynchronized-mutable epoch 
                         ^:unsynchronized-mutable publishers 
                         ^:unsynchronized-mutable subscribers 
                         ^:unsynchronized-mutable read-index]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    (run! pub/stop (flatten-publishers publishers))
    (run! sub/stop subscribers)
    (set! replica-version nil)
    (set! epoch nil)
    (set! publishers nil)
    (set! subscribers nil)
    component)

  m/Messenger
  (id [this] id)

  (publishers [messenger]
    (flatten-publishers publishers))

  (subscribers [messenger]
    subscribers)

  (update-publishers [messenger pub-infos]
    (set! publishers (transition-publishers messenger messenger-group publishers pub-infos))
    messenger)

  (update-subscribers [messenger sub-infos]
    (set! subscribers (transition-subscribers messenger messenger-group subscribers sub-infos))
    messenger)

  (ticket-counters [messenger]
    ticket-counters)

  (set-replica-version! [messenger rv]
    (assert (or (nil? replica-version) (> rv replica-version)) [rv replica-version])
    (set! read-index 0)
    (set! replica-version rv)
    (m/set-epoch! messenger 0))

  (replica-version [messenger]
    replica-version)

  (epoch [messenger]
    epoch)

  (set-epoch! [messenger e]
    (assert (or (nil? epoch) (> e epoch) (zero? e)))
    (set! epoch e)
    (run! #(sub/set-epoch! % e) subscribers)
    messenger)

  (next-epoch! [messenger]
    (m/set-epoch! messenger (inc epoch)))

  (poll [messenger]
    ;; TODO, poll all subscribers in one poll?
    ;; TODO, test for overflow?
    (let [subscriber (get subscribers (mod read-index (count subscribers)))
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
      (loop [pubs (shuffle (get publishers [dst-task-id slot-id]))]
        (if-let [publisher (first pubs)]
          (let [ret (pub/offer! publisher buf)]
            (println "Offer segment" [:ret ret :message message :pub (pub/pub-info publisher)])
            (if (neg? ret)
              (recur (rest pubs))
              task-slot))))))

  #_(offer-heartbeats! [messenger]
    
    
    )

  (poll-recover [messenger]
    (loop [sbs subscribers]
      (let [sub (first sbs)] 
        (when sub 
          (if-not (sub/get-recover sub)
            (sub/poll-replica! sub)
            (println "poll-recover!, has barrier, skip:" (sub/sub-info sub)))
          (recur (rest sbs)))))
    (if (empty? (remove sub/get-recover subscribers))
      (let [recover (sub/get-recover (first subscribers))] 
        (assert recover)
        (assert (= 1 (count (set (map sub/get-recover subscribers)))) 
                "All subscriptions should recover at same checkpoint.")
        recover)))

  (offer-barrier [messenger pub-info]
    (onyx.messaging.protocols.messenger/offer-barrier messenger pub-info {}))

  (offer-barrier [messenger publisher barrier-opts]
    (let [dst-task-id (.dst-task-id publisher)
          slot-id (.slot-id publisher)
          _ (assert slot-id)
          barrier (merge (->Barrier id dst-task-id slot-id (m/replica-version messenger) (m/epoch messenger))
                         barrier-opts
                         {;; Extra debug info
                          :site (.site publisher)
                          :stream (.stream-id publisher)
                          :new-id (java.util.UUID/randomUUID)})
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress barrier))]
      (let [ret (pub/offer! publisher buf)] 
        (println "Offer barrier:" [:ret ret :message barrier :pub (pub/pub-info publisher)])
        ret)))

  (unblock-subscribers! [messenger]
    (run! sub/unblock! subscribers)
    messenger)

  (barriers-aligned? [messenger]
    (empty? (remove sub/blocked? subscribers)))

  (all-barriers-completed? [messenger]
    (empty? (remove sub/completed? subscribers))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (->AeronMessenger messenger-group id (:ticket-counters messenger-group) nil nil nil nil 0))
