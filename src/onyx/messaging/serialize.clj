(ns onyx.messaging.serialize
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.serialization MessageEncoder MessageDecoder MessageEncoder$SegmentsEncoder]))

(def message-id ^:const (byte 0))
(def barrier-id ^:const (byte 1))
(def heartbeat-id ^:const (byte 2))
(def ready-id ^:const (byte 3))
(def ready-reply-id ^:const (byte 4))

; (defn message [replica-version short-id payload]
;   {:type message-id :replica-version replica-version :short-id short-id :payload payload})

(defn barrier [replica-version epoch short-id]
  {:type barrier-id :replica-version replica-version :epoch epoch :short-id short-id})

;; should be able to get rid of src-peer-id via short-id
(defn ready [replica-version src-peer-id short-id]
  {:type ready-id :replica-version replica-version :src-peer-id src-peer-id :short-id short-id})

(defn ready-reply [replica-version src-peer-id dst-peer-id session-id short-id]
  {:type ready-reply-id :replica-version replica-version :src-peer-id src-peer-id 
   :dst-peer-id dst-peer-id :session-id session-id :short-id short-id})

(defn heartbeat [replica-version epoch src-peer-id dst-peer-id session-id short-id]
  {:type heartbeat-id :replica-version replica-version :epoch epoch 
   :src-peer-id src-peer-id :dst-peer-id dst-peer-id :session-id session-id
   :short-id short-id})

(defn get-message-type [^UnsafeBuffer buf offset]
  (.getByte buf ^long offset))

(defn put-message-type [^UnsafeBuffer buf offset type-id] 
  (.putByte buf offset type-id))

(defn serialize ^UnsafeBuffer [msg]
  (let [bs ^bytes (messaging-compress msg)
        length (inc (alength bs))
        buf (UnsafeBuffer. (byte-array length))]
    (put-message-type buf 0 (:type msg))
    (.putBytes buf 1 bs)
    buf))  

(defn deserialize [^UnsafeBuffer buf offset length]
  (let [bs (byte-array length)] 
    (.getBytes buf offset bs)
    (messaging-decompress bs)))

(defn add-segment-payload! [^MessageEncoder encoder segments]
  (let [seg-encoder (loop [^MessageEncoder$SegmentsEncoder enc (.segmentsCount encoder (count segments))
                           v (first segments) 
                           vs (rest segments)]
                      (let [bs ^bytes (messaging-compress v) 
                            cnt ^int (alength bs)]
                        (when v 
                          (recur (.putSegmentBytes (.next enc) bs 0 cnt)
                                 (first vs) 
                                 (rest vs)))))]
    (.encodedLength encoder)))

(defn wrap-message-encoder ^MessageEncoder [^UnsafeBuffer buf offset]
  (-> (MessageEncoder.)
      (.wrap buf offset)))

(defn wrap-message-decoder ^MessageDecoder [^UnsafeBuffer buf offset]
  (-> (MessageDecoder.)
      (.wrap buf offset MessageDecoder/BLOCK_LENGTH 0)))

(defn into-segments! [^MessageDecoder decoder segments]
  ;; FIXME preallocate earlier. Bad to reallocate.
  ;; I think it's legit to preallocate this byte array and re-use it. 
  ;; Nippy knows byte lengths, so we don't need it to be exactly the right size
  ;; For now allocate to the whole encoded length. Excessive.
  (let [bs (byte-array (.encodedLength decoder))] 
    (loop [dc (.segments decoder) cnt 0]
      (when (.hasNext dc)
        ;; can use an unsafe buffer here, but we have no way to read from it with
        ;; nippy without copying to another byte array
        ;; Ideally nippy could read directly from the mutable buffers without a copy
        (.getSegmentBytes dc bs 0 (.segmentBytesLength dc))
        (conj! segments 
               (messaging-decompress bs))
        (recur (.next dc) (inc cnt)))))
  segments)
