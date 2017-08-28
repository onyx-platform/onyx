(ns ^{:no-doc true} onyx.state.serializers.windowing-key-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]
           [java.nio.ByteOrder]))

(defprotocol PDecoder
  (wrap-impl [this bs])
  (get-type [this])
  (get-state-idx [this])
  (get-group-id [this])
  (get-extent [this])
  (length [this]))

(deftype GroupedTriggerDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-extent [this]
    (throw (Exception. "not implemented")))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (let [bs (byte-array 8)]
      (.getBytes buffer (unchecked-add-int offset 2) bs)
      bs))
  (length [this] 10))

(deftype GroupedNoExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (let [bs (byte-array 8)]
      (.getBytes buffer (unchecked-add-int offset 2) bs)
      bs))
  (get-extent [this] 1)
  (length [this] 10))

(deftype GroupedLongExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (let [bs (byte-array 8)]
      (.getBytes buffer (unchecked-add-int offset 2) bs)
      bs))
  (get-extent [this]
    (.getLong buffer (unchecked-add-int offset 10) java.nio.ByteOrder/BIG_ENDIAN))
  (length [this] 18))

(deftype GroupedLongLongExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this]
    (let [bs (byte-array 8)]
      (.getBytes buffer (unchecked-add-int offset 2) bs)
      bs))
  (get-extent [this]
    [(.getLong buffer (unchecked-add-int offset 10) java.nio.ByteOrder/BIG_ENDIAN)
     (.getLong buffer (unchecked-add-int offset 18) java.nio.ByteOrder/BIG_ENDIAN)])
  (length [this] 26))

(deftype UngroupedTriggerDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-extent [this]
    (throw (Exception. "not implemented")))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this])
  (length [this] 2))

(deftype UngroupedNoExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this] nil)
  ;; NOTE: get-extent always returns 1 as this is what global windows expect
  (get-extent [this] 1)
  (length [this] 2))

(deftype UngroupedLongExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this] nil)
  (get-extent [this]
    (.getLong buffer 2 java.nio.ByteOrder/BIG_ENDIAN))
  (length [this] 10))

(deftype UngroupedLongLongExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-id [this] nil)
  (get-extent [this]
    [(.getLong buffer (unchecked-add-int offset 2) java.nio.ByteOrder/BIG_ENDIAN)
     (.getLong buffer (unchecked-add-int offset 10) java.nio.ByteOrder/BIG_ENDIAN)])
  (length [this]
    18))

(defn grouped-trigger [buffer offset]
  (->GroupedTriggerDecoder buffer offset))

(defn grouped-no-extent [buffer offset]
  (->GroupedNoExtentDecoder buffer offset))

(defn grouped-long-extent [buffer offset]
  (->GroupedLongExtentDecoder buffer offset))

(defn grouped-long-long-extent [buffer offset]
  (->GroupedLongLongExtentDecoder buffer offset))

(defn ungrouped-trigger [buffer offset]
  (->UngroupedTriggerDecoder buffer offset))

(defn ungrouped-no-extent [buffer offset]
  (->UngroupedNoExtentDecoder buffer offset))

(defn ungrouped-long-extent [buffer offset]
  (->UngroupedLongExtentDecoder buffer offset))

(defn ungrouped-long-long-extent [buffer offset]
  (->UngroupedLongLongExtentDecoder buffer offset))
