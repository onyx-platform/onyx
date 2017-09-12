(ns onyx.state.serializers.group-reverse-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (wrap-impl [this bs])
  (get-type [this])
  (get-state-idx [this])
  (get-group-id [this])
  (length [this]))

(deftype GroupReverseDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (.getLong buffer (unchecked-add-int offset 2)))
  (length [this] 10))


