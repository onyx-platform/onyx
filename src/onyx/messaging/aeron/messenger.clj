(ns ^:no-doc onyx.messaging.aeron.messenger
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn trace] :as timbre]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes]]
            [onyx.messaging.aeron.utils :as autil :refer [action->kw stream-id heartbeat-stream-id]]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.protocols.subscriber :as sub]
            [onyx.messaging.aeron.subscriber :refer [new-subscription]]
            [onyx.messaging.aeron.publisher :refer [reconcile-pub]]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.static.default-vals :refer [arg-or-default]])
  (:import [io.aeron Aeron Aeron$Context Publication Subscription]
           [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [onyx.messaging.aeron.publisher Publisher]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

;; TODO:
;; Use java.util.concurrent.atomic.AtomicLong for tickets
;(def al (java.util.concurrent.atomic.AtomicLong.)) 

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id job-id task-id peer-site peer-sites]
  {:aeron/peer-task-id nil})

(defmethod m/get-peer-site :aeron
  [peer-config]
  {:address (common/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

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

(defn transition-publishers [peer-config messenger publishers pub-infos]
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
                   (reconcile-pub peer-config old new))))
         (group-by (fn [^Publisher pub]
                     [(.dst-task-id pub) (.slot-id pub)])))))

(deftype AeronMessenger [messenger-group 
                         id 
                         ticket-counters 
                         ^:unsynchronized-mutable replica-version 
                         ^:unsynchronized-mutable epoch 
                         ^:unsynchronized-mutable publishers 
                         ^:unsynchronized-mutable subscriber]
  component/Lifecycle
  (start [component]
    component)

  (stop [component]
    (run! pub/stop (flatten-publishers publishers))
    (when subscriber (sub/stop subscriber))
    (set! replica-version nil)
    (set! epoch nil)
    (set! publishers nil)
    (set! subscriber nil)
    component)

  m/Messenger
  (id [this] id)

  (publishers [messenger]
    (flatten-publishers publishers))

  (subscriber [messenger]
    subscriber)

  (info [messenger]
    {:ticket-counters @ticket-counters
     :replica-version replica-version
     :epoch epoch
     :channel (autil/channel (:peer-config messenger-group))
     :publishers (mapv pub/info (m/publishers messenger))
     :subscriber (sub/info subscriber)})

  (update-publishers [messenger pub-infos]
    (set! publishers (transition-publishers (:peer-config messenger-group) 
                                            messenger publishers pub-infos))
    messenger)

  (update-subscriber [messenger sub-info]
    (assert sub-info)
    (set! subscriber 
          (sub/update-sources! 
           (or subscriber
               (sub/start 
                (new-subscription (:peer-config messenger-group) 
                                  id
                                  ticket-counters
                                  sub-info)))
           (:sources sub-info)))
    messenger)

  (ticket-counters [messenger]
    ticket-counters)

  (set-replica-version! [messenger rv]
    (assert (or (nil? replica-version) (> rv replica-version)) [rv replica-version])
    (set! replica-version rv)
    (when subscriber (sub/set-replica-version! subscriber rv))
    (run! #(pub/set-replica-version! % rv) (m/publishers messenger))
    messenger)

  (replica-version [messenger]
    replica-version)

  (epoch [messenger]
    epoch)

  (set-epoch! [messenger e]
    (assert (or (nil? epoch) (> e epoch) (zero? e)))
    (set! epoch e)
    (when subscriber (sub/set-epoch! subscriber e))
    (run! #(pub/set-epoch! % e) (m/publishers messenger))
    messenger)

  (poll [messenger]
    (mapv t/input (sub/poll! subscriber)))

  (offer-segments [messenger batch {:keys [dst-task-id slot-id short-id] :as task-slot}]
    ;; ideally this would take the short id and get the right publisher
    (loop [pubs (shuffle (get publishers [dst-task-id slot-id]))]
      ;; TODO SERIALIZE ONCE, TRY MULTIPLE TIMES, WILL NEED TO MODIFY THE SHORT-ID IN THE PAYLOAD THOUGH
      (if-let [^Publisher publisher (first pubs)]
        (let [message (t/message replica-version (pub/short-id publisher) batch)
              payload ^bytes (messaging-compress message)
              buf ^UnsafeBuffer (UnsafeBuffer. payload)
              ret (pub/offer! publisher buf epoch)]
          ;; TODO, retry here a couple more times before trying others
          ;; retain compressed version for retry
          (debug "Offer segment" [:ret ret :message message :pub (pub/info publisher)])
          (if (neg? ret)
            (recur (rest pubs))
            task-slot)))))

  (offer-barrier [messenger pub-info]
    (onyx.messaging.protocols.messenger/offer-barrier messenger pub-info {}))

  (offer-barrier [messenger publisher barrier-opts]
    (let [barrier (merge (t/barrier replica-version epoch (pub/short-id publisher)) barrier-opts)
          buf ^UnsafeBuffer (UnsafeBuffer. ^bytes (messaging-compress barrier))]
      (let [ret (pub/offer! publisher buf (dec epoch))] 
        (debug "Offer barrier:" [:ret ret :message barrier :pub (pub/info publisher)])
        ret))))

(defmethod m/build-messenger :aeron [peer-config messenger-group id]
  (->AeronMessenger messenger-group id (:ticket-counters messenger-group) nil nil nil nil))
