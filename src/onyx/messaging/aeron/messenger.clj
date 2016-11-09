(ns ^:no-doc onyx.messaging.aeron.messenger
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn] :as timbre]
            [onyx.messaging.aeron.peer-manager :as pm]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes map->Barrier ->Message 
                                      ->Barrier ->Ready ->ReadyReply ->Heartbeat]]
            [onyx.messaging.messenger :as m]
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

(defn hash-sub [sub-info]
  (hash (select-keys sub-info [:src-peer-id :dst-task-id :slot-id :site])))


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

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id task-id peer-site peer-sites]
  {}
  #_{:aeron/peer-task-id (allocate-id (hash [peer-id task-id]) peer-site peer-sites)})

(defmethod m/get-peer-site :aeron
  [peer-config]
  (println "GET PEER SITE" (common/external-addr peer-config))
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

(defn flatten-publishers [publications]
  (reduce into [] (vals publications)))

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
  (id [this] id)

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

  (lookup-ticket [messenger src-peer-id session-id]
    ;; when to unregister ticket?
    ;; Do we want it to get deallocated when unavailable-image once all subscribers are gone?


    ;; FIXME, needs to also be matched by src-peer-id
    ;; Then we can dissoc each time src-peer-id is reallocated / goes away
    ;; If we have multiple peer ids that are the same sending to same peer they'll be on the same image otherwise
    (-> ticket-counters
        (swap! update-in 
               [src-peer-id session-id]
               (fn [ticket]
                 (or ticket (atom -1))))
        (get-in [src-peer-id session-id])))

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
  (offer-heartbeats [messenger]
    
    
    )

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
    (let [barrier (merge (->Barrier id (.dst-task-id publisher) (m/replica-version messenger) (m/epoch messenger))
                         (assoc barrier-opts 
                                ;; Extra debug info
                                :slot-id (.slot-id publisher)
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
