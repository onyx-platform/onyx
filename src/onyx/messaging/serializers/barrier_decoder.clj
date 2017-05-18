(ns ^{:no-doc true} onyx.messaging.serializers.barrier-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-epoch [this])
  (get-opts-map-bytes [this]))

(deftype Decoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-epoch [this]
    (.getLong buffer offset))
  (get-opts-map-bytes [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 8)))] 
      (.getBytes buffer (unchecked-add-int offset 10) bs)
      bs)))

(defn wrap [buffer offset]
  (->Decoder buffer offset))
