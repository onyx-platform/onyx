(ns onyx.plugin.messaging-output
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.serialize :as sz]
            [onyx.peer.constants :refer [load-balance-slot-id]]
            [onyx.peer.grouping :as g]
            [net.cgrand.xforms :as x]
            [onyx.plugin.protocols.plugin :as op]
            [onyx.plugin.protocols.output :as oo]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t])
  (:import [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy]
           [java.util.concurrent.atomic AtomicLong]
           [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]))

(defn offer-segments [replica-version epoch ^MessageEncoder encoder buffer batch publisher]
  (let [encoder (-> encoder
                    ;; offset by 1 byte, as message type is encoded
                    (.wrap buffer 1)
                    (.replicaVersion replica-version)) 
        length (sz/add-segment-payload! encoder batch)] 
    (let [encoder (.destId encoder (pub/short-id publisher))
          ret (pub/offer! publisher buffer (inc length) epoch)]
      (debug "Offer segment" [:ret ret :batch batch :pub (pub/info publisher)])
      (if (neg? ret)
        0 
        length))))

;; TODO: split out destinations for retry, may need to switch destinations, can
;; do every thing in a single offer 
;; TODO: be smart about sending messages to multiple co-located tasks
(defn send-messages [messenger ^MessageEncoder encoder buffer prepared]
  (let [replica-version (m/replica-version messenger)
        epoch (m/epoch messenger)] 
    (loop [batches prepared 
           bytes-sent 0]
      (if-let [[pub batch] (first batches)] 
        (let [encoder (.wrap encoder buffer 1)
              ret (offer-segments replica-version epoch encoder buffer batch pub)]
          (if (pos? ret)
            (recur (rest batches) 
                   (unchecked-add-int bytes-sent ret))
            [batches bytes-sent]))
        [nil bytes-sent]))))

(defn partition-xf [publisher write-batch-size]
  ;; Ideally, write batching would be in terms of numbers of bytes. 
  ;; We should serialize message by message ahead of time until we hit the cut-off point
  ;; Output batch size should also be capped at the batch size of the downstream task
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

(deftype MessengerOutput [^:unsynchronized-mutable remaining ^MessageEncoder encoder 
                          ^UnsafeBuffer buffer ^long write-batch-size 
                          ^java.util.ArrayList flattened ^AtomicLong written-bytes]
  op/Plugin
  (start [this event] this)

  (stop [this event] this)

  oo/Output
  (synced? [this _]
    true)

  (prepare-batch [this {:keys [onyx.core/results onyx.core/triggered task->group-by-fn] :as event} 
                  replica messenger]
    (let [get-pub-fn (if (empty? task->group-by-fn)
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
      (set! remaining final-output)
      true))

  (write-batch [this event _ messenger]
    (let [[left bytes-sent] (send-messages messenger encoder buffer remaining)]
      (.addAndGet written-bytes bytes-sent)
      (if (empty? remaining)
        (do (set! remaining nil)
            true)
        (do (set! remaining left)
            false)))))

(defn new-messenger-output [{:keys [onyx.core/task-map onyx.core/monitoring] :as event}]
  (let [write-batch-size (or (:onyx/batch-write-size task-map) (:onyx/batch-size task-map))
        ;; FIXME: should be configured via information model
        bs (byte-array 10000000) 
        buffer (UnsafeBuffer. bs)
        tmp-storage (java.util.ArrayList. 2000)]
    ;; set message type in buffer early, as we will be re-using the buffer
    (sz/put-message-type buffer 0 sz/message-id)
    (->MessengerOutput nil (MessageEncoder.) buffer (long write-batch-size)
                       tmp-storage (:written-bytes monitoring))))
