(ns onyx.state.serializers.group-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (wrap-impl [this bs])
  (get-type [this])
  (get-state-idx [this])
  (get-group [this])
  (get-group-len [this])
  (length [this]))

(defn get-group-id [^bytes bs]
  (.getLong (UnsafeBuffer. bs) 0))

(deftype GroupDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
  (get-group [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 2)))] 
      (.getBytes buffer (unchecked-add-int offset 4) bs)
      bs))
  (length [this]
    (unchecked-add-int 4 (get-group-len this))))


