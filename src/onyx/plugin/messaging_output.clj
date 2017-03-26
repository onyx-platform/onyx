(ns onyx.plugin.messaging-output
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.peer.constants :refer [load-balance-slot-id]]
            [onyx.peer.grouping :as g]
            [onyx.messaging.serialize :as sz]
            [onyx.messaging.serializers.segment-encoder :as segment-encoder]
            [onyx.messaging.serializers.base-encoder :as base-encoder]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :refer [max-message-length]]
            [net.cgrand.xforms :as x]
            [onyx.plugin.protocols :as p]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t])
  (:import [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy]))

(defn offer-segments [replica-version epoch buffer batch publisher]
  (let [start-offset 0
        base-enc (-> (base-encoder/->Encoder buffer start-offset)
                     (base-encoder/set-type sz/message-id)
                     (base-encoder/set-replica-version replica-version)
                     (base-encoder/set-dest-id (pub/short-id publisher)))
        base-len (base-encoder/length base-enc)
        segment-enc (-> (segment-encoder/->Encoder buffer nil nil)
                        (segment-encoder/wrap (unchecked-add-int base-len start-offset)))
        segment-enc (reduce (fn [enc segment]
                              (segment-encoder/add-message enc (messaging-compress segment)))
                            segment-enc
                            batch)
        payload-len (- (segment-encoder/offset segment-enc) base-len)
        base-enc (base-encoder/set-payload-length base-enc payload-len)
        length (- (segment-encoder/offset segment-enc) start-offset)] 
    (let [ret (pub/offer! publisher buffer length epoch)]
      (debug "Offer segment" [:ret ret :batch batch :pub (pub/info publisher)])
      (if (neg? ret)
        0 
        length))))

;; TODO: split out destinations for retry, may need to switch destinations, can
;; do every thing in a single offer.
;; TODO: be smart about sending messages to multiple co-located tasks
(defn send-messages [messenger buffer prepared]
  (let [replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)] 
    (loop [batches prepared]
      (if-let [[pub batch] (first batches)] 
        (let [ret (offer-segments replica-version epoch buffer batch pub)]
          (if (pos? ret)
            (recur (rest batches))
            batches))
        nil))))

(defn partition-xf [publisher write-batch-size]
  ;; Ideally, write batching would be in terms of numbers of bytes. 
  ;; We should serialize message by message ahead of time until we hit the cut-off point.
  ;; Output batch size should also be capped at the batch size of the downstream task.
  (comp (map first)
        (partition-all write-batch-size)
        (map (fn [segments]
               (list publisher segments)))))

(defn add-segment [^java.util.ArrayList flattened segment event result get-pub-fn]
  (let [routes (r/route-data event result segment)
        segment* (r/flow-conditions-transform segment routes event)]
    (run! (fn [route]
            (.add flattened 
                  (list segment* 
                        (get-pub-fn segment* route))))
          (:flow routes))))

(deftype MessengerOutput [^:unsynchronized-mutable buffered ^UnsafeBuffer buffer
                          ^long write-batch-size ^java.util.ArrayList flattened]
  p/Plugin
  (start [this event] this)
  (stop [this event] this)

  p/Checkpointed
  (recover! [this _ _] this)
  (checkpoint [_])
  (checkpointed! [_ _])

  p/BarrierSynchronization
  (synced? [this _]
    true)
  (completed? [this] (empty? buffered))

  p/Output
  (prepare-batch [this {:keys [onyx.core/results onyx.core/triggered task->group-by-fn] :as event} 
                  replica messenger]
    (let [;; generate this on each new replica / messenger
          get-pub-fn (if (empty? task->group-by-fn)
                       (fn [segment dst-task-id]
                         (rand-nth (m/task->publishers messenger dst-task-id)))            
                       (fn [segment dst-task-id]
                         (let [group-fn (task->group-by-fn dst-task-id) 
                               hsh (hash (group-fn segment))
                               dest-pubs (m/task->publishers messenger dst-task-id)]
                           (get dest-pubs (mod hsh (count dest-pubs))))))
          _ (run! (fn [{:keys [leaves] :as result}]
                    (run! (fn [seg]
                            (add-segment flattened seg event result get-pub-fn))
                          leaves))
                  (:tree results))
          _ (run! (fn [seg] 
                    (add-segment flattened seg event {:leaves [seg]} get-pub-fn))
                  triggered)
          xf (comp (x/by-key second (x/into []))
                   (mapcat (fn [[pub coll]]
                             (sequence (partition-xf pub write-batch-size) 
                                       coll))))
          final-output (sequence xf flattened)]
      (.clear ^java.util.ArrayList flattened)
      (set! buffered final-output)
      true))

  (write-batch [this event _ messenger]
    (let [remaining (send-messages messenger buffer buffered)]
      (if (empty? remaining)
        (do (set! buffered nil)
            true)
        (do (set! buffered remaining)
            false)))))

(defn new-messenger-output [{:keys [onyx.core/task-map] :as event}]
  (let [write-batch-size (or (:onyx/batch-write-size task-map) (:onyx/batch-size task-map))
        bs (byte-array (max-message-length)) 
        buffer (UnsafeBuffer. bs)
        tmp-storage (java.util.ArrayList. 2000)]
    ;; set message type in buffer early, as we will be re-using the buffer
    ;;(sz/put-message-type buffer 0 sz/message-id)
    (->MessengerOutput nil buffer (long write-batch-size) tmp-storage)))
