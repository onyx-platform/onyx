(ns onyx.state.serializers.state-entry-key-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-idx [this])
  (get-group-id [this])
  (get-time [this])
  (get-offset [this])
  (wrap-impl [this bs])
  (length [this]))

(deftype GroupedEntryDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (let [bs (byte-array 8)]
      (.getBytes buffer (unchecked-add-int offset 2) bs)
      bs))
  (get-time [this]
    (.getLong buffer (unchecked-add-int offset 10) java.nio.ByteOrder/BIG_ENDIAN))
  (get-offset [this]
    (.getLong buffer (unchecked-add-int offset 18) java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs)))

(deftype UngroupedEntryDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    nil)
  (get-time [this]
    (.getLong buffer (unchecked-add-int offset 2) java.nio.ByteOrder/BIG_ENDIAN))
  (get-offset [this]
    (.getLong buffer (unchecked-add-int offset 10) java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs)))

(deftype GroupedGlobalEntryDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (let [bs (byte-array 8)]
      (.getBytes buffer (unchecked-add-int offset 2) bs)
      bs))
  (get-time [this] 0)
  (get-offset [this]
    (.getLong buffer (unchecked-add-int offset 10) java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs)))

(deftype UngroupedGlobalEntryDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    nil)
  (get-time [this] 0)
  (get-offset [this]
    (.getLong buffer (unchecked-add-int offset 2) java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs)))
