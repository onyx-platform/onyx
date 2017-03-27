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

(defn add-segment [^java.util.ArrayList flattened segment event result get-pub-fn]
  (let [routes (r/route-data event result segment)
        segment* (r/flow-conditions-transform segment routes event)]
    (run! (fn [route]
            (.add flattened 
                  (list segment* 
                        (get-pub-fn segment* route))))
          (:flow routes))))

(defn offer-segments! [pub epoch]
  (let [base-enc ^onyx.messaging.serializers.base_encoder.Encoder (pub/base-encoder pub)
        enc (pub/segment-encoder pub)]
    (if-not (zero? (segment-encoder/segment-count enc))
      (let [segments-len (- (segment-encoder/offset enc)
                            (base-encoder/length base-enc))] 
       (base-encoder/set-payload-length base-enc segments-len)
       (pub/offer! pub (.buffer base-enc) (segment-encoder/offset enc) epoch))
      ;; fake success code, as we don't have any more segments to send
      1)))

(defn try-sends [pubs epoch]
  (loop [pubs pubs]
    (if-let [pub (first pubs)]
      (if (neg? (offer-segments! pub epoch))
        pubs
        (recur (rest pubs))))))

(defn reset-encoder! [pub]
  (segment-encoder/wrap (pub/segment-encoder pub)
                        (base-encoder/length (pub/base-encoder pub))))

(defn encode-segment [segment-encoder bs]
  (if (segment-encoder/has-capacity? segment-encoder (alength ^bytes bs))
    (do (segment-encoder/add-message segment-encoder bs)
        true)
    false))

(deftype MessengerOutput [^long write-batch-size 
                          ^java.util.ArrayList segments 
                          ^:unsynchronized-mutable ^java.util.Iterator iterator
                          ^:unsynchronized-mutable queued-offers
                          ^:unsynchronized-mutable add-failed]
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
  (completed? [this] (empty? segments))

  p/Output
  (prepare-batch [this {:keys [onyx.core/results onyx.core/triggered task->group-by-fn] :as event} 
                  replica messenger]
    (.clear ^java.util.ArrayList segments)
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
                            (add-segment segments seg event result get-pub-fn))
                          leaves))
                  (:tree results))
          _ (run! (fn [seg] 
                    (add-segment segments seg event {:leaves [seg]} get-pub-fn))
                  triggered)]
      (run! reset-encoder! (m/publishers messenger))
      (set! iterator (.iterator segments))
      true))

  (write-batch [this event replica messenger]
    (cond add-failed
          (let [[publisher segment-bytes] add-failed]
            (if (neg? (offer-segments! publisher (m/epoch messenger)))
              false
              (do (reset-encoder! publisher)
                  (when-not (encode-segment (pub/segment-encoder publisher) segment-bytes)
                    (throw (ex-info "Aeron buffer size is not big enough to contain the segment.
                                     Please increase the term buffer length via java property aeron.term.buffer.length"
                                    {:max-message-size (max-message-length)
                                     :message-length (alength ^bytes segment-bytes)})))
                  (set! add-failed nil)
                  (p/write-batch this event replica messenger))))

          (.hasNext iterator)
          (loop [] 
            (if (.hasNext iterator)
              (let [[segment publisher] (.next iterator)
                    bs (messaging-compress segment)]
                (if (encode-segment (pub/segment-encoder publisher) bs)
                  (recur)
                  (do (set! add-failed (list publisher bs))
                      (p/write-batch this event replica messenger))))
              (p/write-batch this event replica messenger)))

          ;; move to final phase where we send main buffers out
          (and (not (.hasNext iterator))
               (empty? queued-offers))
          (do (set! queued-offers (m/publishers messenger))
              (p/write-batch this event replica messenger))

          :else
          (let [remaining (try-sends queued-offers (m/epoch messenger))]
            (set! queued-offers remaining)
            (if (empty? remaining)
              (do (.clear segments)
                  true)
              false)))))

(defn new-messenger-output [{:keys [onyx.core/task-map] :as event}]
  (let [write-batch-size (or (:onyx/batch-write-size task-map) (:onyx/batch-size task-map))
        bs (byte-array (max-message-length)) 
        segments (java.util.ArrayList. 2000)]
    (->MessengerOutput (long write-batch-size) segments nil nil nil)))
