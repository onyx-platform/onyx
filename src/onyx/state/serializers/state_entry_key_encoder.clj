(ns onyx.state.serializers.state-entry-key-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-state-idx [this idx])
  (set-group [this group])
  (set-time [this time])
  (set-offset [this offset-value])
  (get-bytes [this])
  (wrap-impl [this bs])
  (length [this]))

(deftype GroupedEntryEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (set-time [this t]
    (.putLong buffer (unchecked-add-int offset 10) (long t) java.nio.ByteOrder/BIG_ENDIAN))
  (set-offset [this offset-value]
    (.putLong buffer (unchecked-add-int offset 18) offset-value java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array 26)]
      (.getBytes buffer offset ^bytes ret-bs)
      ret-bs)))

(deftype UngroupedEntryEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group [this group-id])
  (set-time [this t]
    (.putLong buffer (unchecked-add-int offset 2) ^long t java.nio.ByteOrder/BIG_ENDIAN))
  (set-offset [this offset-value]
    (.putLong buffer (unchecked-add-int offset 10) offset-value java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array 18)]
      (.getBytes buffer offset ^bytes ret-bs)
      ret-bs)))

(deftype UngroupedGlobalEntryEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group [this group-id])
  (set-time [this t])
  (set-offset [this offset-value]
    (.putLong buffer (unchecked-add-int offset 2) offset-value java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array 10)]
      (.getBytes buffer offset ^bytes ret-bs)
      ret-bs)))

(deftype GroupedGlobalEntryEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (set-time [this t])
  (set-offset [this offset-value]
    (.putLong buffer (unchecked-add-int offset 10) offset-value java.nio.ByteOrder/BIG_ENDIAN))
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array 18)]
      (.getBytes buffer offset ^bytes ret-bs)
      ret-bs)))
