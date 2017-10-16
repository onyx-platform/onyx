(ns onyx.plugin.messaging-output
  (:require [clojure.core.async :refer [alts!! <!! >!! <! >! poll! timeout chan close! thread go]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.messaging.protocols.messenger :as m]
            [onyx.messaging.protocols.publisher :as pub]
            [onyx.messaging.aeron.publisher]
            [onyx.peer.constants :refer [load-balance-slot-id]]
            [onyx.peer.grouping :as g]
            [onyx.messaging.serialize :as sz]
            [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.aeron.utils :refer [max-message-length]]
            [onyx.plugin.protocols :as p]
            [onyx.protocol.task-state :refer :all]
            [clj-tuple :as t])
  (:import [org.agrona.concurrent UnsafeBuffer IdleStrategy BackoffIdleStrategy]
           [java.util.concurrent.atomic AtomicInteger]))

(defn add-segment [#^"[Ljava.lang.Object;" segments 
                   #^"[Lclojure.lang.Keyword;" stored-routes 
                   ^AtomicInteger size 
                   segment 
                   event
                   result]
  (let [routes (r/route-data event result segment)
        segment* (r/flow-conditions-transform segment routes event)]
    (run! (fn [route]
            (let [i (.getAndIncrement size)]
              (aset segments i segment*)
              (aset stored-routes i route)))
          (:flow routes))))

(defn try-sends [pubs]
  (loop [pubs pubs]
    (if-let [pub (first pubs)]
      (if (neg? (pub/offer-segments! pub))
        pubs
        (do (pub/reset-segment-encoder! pub)
            (recur (rest pubs)))))))

(deftype MessengerOutput [#^"[Ljava.lang.Object;" segments 
                          #^"[Lclojure.lang.Keyword;" routes
                          ^AtomicInteger idx
                          ^AtomicInteger size
                          ^:unsynchronized-mutable queued-offers
                          ^bytes ^:unsynchronized-mutable failed-add
                          ^:unsynchronized-mutable full-pub]
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
  (completed? [this]
    (and (= (.get idx) (.get size))
         (nil? full-pub)
         (empty? queued-offers)))
  p/Output
  (prepare-batch [this {:keys [onyx.core/results onyx.core/triggered] :as event} replica messenger]
    (.set size 0)
    (run! (fn [result]
            (run! (fn [seg]
                    (add-segment segments routes size seg event result))
                  (:leaves result)))
          (:tree results))
    (run! (fn [seg] 
            (add-segment segments routes size seg event {:leaves [seg]}))
          triggered)
    (run! pub/reset-segment-encoder! (m/publishers messenger))
    (set! queued-offers nil)
    (set! full-pub nil)
    (.set idx 0)
    true)

  (write-batch [this event replica messenger]
    (let [size-val (.get size)] 
      (if (zero? size-val)
        true
        (let [dst-task->publisher (m/task->publishers messenger)
              get-pub-fn (fn [dst-task-id] (get dst-task->publisher dst-task-id))]
          (cond full-pub
                ;; previous addition of segment to buffer resulted in an overflow
                ;; offer the full buffer, and then add the serialized segment to the reset buffer
                 (if (neg? (pub/offer-segments! full-pub))
                  false
                  (do (pub/reset-segment-encoder! full-pub)
                      ;; publisher already has the last segment saved, lets encode it now
                      (when-not (pub/encode-segment! full-pub failed-add)
                        (throw (ex-info "Aeron buffer size is not big enough to contain the segment.
                                         Please increase the term buffer length via java property aeron.term.buffer.length"
                                        {})))
                      (set! failed-add nil)
                      (set! full-pub nil)
                      (recur event replica messenger)))

                (< (.get idx) size-val)
                (do (loop [i (.get idx)] 
                      (if (< i size-val)
                        ;; add segment to corresponding buffer in publication
                        (let [segment (aget segments i)
                              route (aget routes i)
                              publisher ((get-pub-fn route) segment)
                              serialized (pub/serialize publisher segment)
                              next-idx (.incrementAndGet idx)]
                          (if (pub/encode-segment! publisher serialized)
                            (recur next-idx)
                            (do (set! failed-add serialized)
                                (set! full-pub publisher))))))
                    (recur event replica messenger))

                :else
                (let [_ (when (and (nil? queued-offers) (= (.get idx) size-val))
                          (set! queued-offers (m/publishers messenger)))
                      remaining (try-sends queued-offers)]
                  (set! queued-offers remaining)
                  (if (empty? remaining)
                    (do (run! (fn [i] 
                                (aset segments i nil)
                                (aset routes i nil))
                              (range size-val))
                        true)
                    false))))))))

(defn new-messenger-output [{:keys [onyx.core/task-map] :as event}]
  (let [bs (byte-array (max-message-length)) 
        segments (object-array 20000)
        routes (make-array clojure.lang.Keyword 20000)]
    (->MessengerOutput segments routes (AtomicInteger. 0) (AtomicInteger. 0) nil nil nil)))
