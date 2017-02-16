(ns ^:no-doc onyx.messaging.aeron.messenger
  (:require [clojure.set :refer [subset?]]
            [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal info debug warn trace] :as timbre]
            [onyx.messaging.common :as common]
            [onyx.types :as t :refer [->MonitorEventBytes]]
            [onyx.messaging.serialize :as sz]
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

;; TODO, make sure no stream-id collision issues
(defmethod m/assign-task-resources :aeron
  [replica peer-id job-id task-id peer-site peer-sites]
  {:aeron/peer-task-id nil})

(defmethod m/get-peer-site :aeron
  [peer-config]
  {:address (common/external-addr peer-config)
   :port (:onyx.messaging/peer-port peer-config)})

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
                         monitoring
                         id 
                         ticket-counters 
                         ^:unsynchronized-mutable replica-version 
                         ^:unsynchronized-mutable epoch 
                         ^:unsynchronized-mutable publishers 
                         ^:unsynchronized-mutable task-publishers 
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
    (set! task-publishers nil)
    (set! subscriber nil)
    component)

  m/Messenger
  (id [this] id)

  (publishers [messenger]
    (flatten-publishers publishers))

  (task->publishers [messenger dst-task-id]
    (get task-publishers dst-task-id))

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
    (set! task-publishers (->> (flatten-publishers publishers)
                               (group-by #(second (.dst-task-id ^Publisher %)))
                               (map (fn [[k pubs]]
                                      [k (->> pubs
                                              (sort-by #(.slot-id ^Publisher %))
                                              (into []))]))
                               (into {})))
    messenger)

  (update-subscriber [messenger sub-info]
    (assert sub-info)
    (set! subscriber 
          (sub/update-sources! 
           (or subscriber
               (sub/start 
                (new-subscription (:peer-config messenger-group) 
                                  monitoring
                                  id
                                  ticket-counters
                                  sub-info)))
           (:sources sub-info)))
    (assert subscriber)
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
    (assert (or (nil? epoch) (> e epoch) (zero? e) (= -1 e)))
    (set! epoch e)
    (when subscriber (sub/set-epoch! subscriber e))
    (run! #(pub/set-epoch! % e) (m/publishers messenger))
    messenger)

  (poll [messenger]
    (sub/poll! subscriber))

  (offer-barrier [messenger publisher]
    (onyx.messaging.protocols.messenger/offer-barrier messenger publisher {}))

  (offer-barrier [messenger publisher barrier-opts]
    (let [barrier (merge (t/barrier replica-version epoch (pub/short-id publisher)) barrier-opts)
          buf (sz/serialize barrier)]
      (let [ret (pub/offer! publisher buf (.capacity buf) (dec epoch))] 
        (debug "Offer barrier:" [:ret ret :message barrier :pub (pub/info publisher)])
        ret))))

(defmethod m/build-messenger :aeron [peer-config messenger-group monitoring id]
  (->AeronMessenger messenger-group monitoring id 
                    (:ticket-counters messenger-group) 
                    nil nil nil nil nil))
