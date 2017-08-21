(ns ^{:no-doc true} onyx.state.serializers.windowing-key-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]
           [java.nio.ByteOrder]))

;; TODO: try to remove the extra allocation in get-bytes
;; It would be preferable to directly write from the buffer into LMDB
(defprotocol PEncoder
  (set-state-idx [this idx])
  (set-group-id [this group-id])
  (set-min-extent [this] "Set the minimum extent representable in lexicographic order to allow for iteration.")
  (set-extent [this extent])
  (get-bytes [this])
  (wrap-impl [this bs])
  (length [this]))

(defn ^bytes encode-key [enc idx ^bytes group extent]
  (set-state-idx enc idx)
  (set-group-id enc group)
  (set-extent enc extent)
  (get-bytes enc))

(deftype GroupedTriggerEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (length [this] 10)
  (set-extent [this _])
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype GroupedNoExtentEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (set-min-extent [this])
  (set-extent [this _])
  (length [this] 10)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype GroupedLongExtentEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (set-min-extent [this]
    (set-extent this 0))
  (set-extent [this extent]
    (.putLong buffer (unchecked-add-int offset 10) extent java.nio.ByteOrder/BIG_ENDIAN))
  (length [this] 18)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype GroupedLongLongExtentEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this group-id]
    (.putBytes buffer (unchecked-add-int offset 2) group-id))
  (set-min-extent [this]
    (set-extent this [0 0]))
  (set-extent [this ee]
    (let [[extent1 extent2] ee
          extent1-offset (unchecked-add-int offset 10)
          extent2-offset (unchecked-add-int offset 18)]
      (.putLong buffer extent1-offset extent1 java.nio.ByteOrder/BIG_ENDIAN)
      (.putLong buffer extent2-offset extent2 java.nio.ByteOrder/BIG_ENDIAN)))
  (length [this] 26)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype UngroupedTriggerEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (length [this]
    2)
  (set-extent [this _])
  (set-group-id [this _])
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype UngroupedNoExtentEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this _])
  (set-extent [this _])
  (set-min-extent [this])
  (length [this] 2)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype UngroupedLongExtentEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this _])
  (set-min-extent [this]
    (set-extent this 0))
  (set-extent [this extent]
    (.putLong buffer 2 extent java.nio.ByteOrder/BIG_ENDIAN))
  (length [this] 10)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array 10)]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(deftype UngroupedLongLongExtentEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group-id [this _] nil)
  (set-min-extent [this]
    (set-extent this [0 0]))
  (set-extent [this [extent1 extent2]]
    (.putLong buffer 2 extent1 java.nio.ByteOrder/BIG_ENDIAN)
    (.putLong buffer 10 extent2 java.nio.ByteOrder/BIG_ENDIAN))
  (length [this] 18)
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-bytes [this]
    (let [ret-bs (byte-array (length this))]
      (.getBytes buffer 0 ^bytes ret-bs)
      ret-bs)))

(defn grouped-trigger [buffer offset]
  (->GroupedTriggerEncoder buffer offset))

(defn grouped-long-extent [buffer offset]
  (->GroupedLongExtentEncoder buffer offset))

(defn grouped-long-long-extent [buffer offset]
  (->GroupedLongLongExtentEncoder buffer offset))

(defn grouped-no-extent [buffer offset]
  (->GroupedNoExtentEncoder buffer offset))

(defn ungrouped-trigger [buffer offset]
  (->UngroupedTriggerEncoder buffer offset))

(defn ungrouped-no-extent [buffer offset]
  (->UngroupedNoExtentEncoder buffer offset))

(defn ungrouped-long-extent [buffer offset]
  (->UngroupedLongExtentEncoder buffer offset))

(defn ungrouped-long-long-extent [buffer offset]
  (->UngroupedLongLongExtentEncoder buffer offset))
