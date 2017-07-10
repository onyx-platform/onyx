(ns ^{:no-doc true} onyx.state.serializers.windowing-key-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

;; TODO: try to remove the extra allocation in get-bytes
;; It would be preferable to directly write from the buffer into LMDB

(defprotocol PEncoder
  (set-type [this type])
  (set-state-idx [this idx])
  (set-group [this group])
  (set-extent [this extent])
  (get-bytes [this])
  (get-group-len [this])
  (wrap-impl [this bs])
  (length [this]))

(defn ^bytes encode-key [enc idx ^bytes group extent]
  (set-state-idx enc idx)
  (set-group enc group)
  (set-extent enc extent)
  (get-bytes enc))

(deftype GroupedTriggerEncoder [^UnsafeBuffer buffer offset]
  PEncoder
  (set-state-idx [this idx]
    (.putShort buffer offset idx))
  (set-group [this bs]
    (.putShort buffer (unchecked-add-int offset 2) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 4) ^bytes bs))
  (length [this]
    (unchecked-add-int 4 (get-group-len this)))
  (set-extent [this _])
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
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
  (set-group [this bs]
    (.putShort buffer (unchecked-add-int offset 2) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 4) ^bytes bs))
  (set-extent [this _])
  (length [this]
    (unchecked-add-int 4 (get-group-len this)))
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
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
  (set-group [this bs]
    (.putShort buffer (unchecked-add-int offset 2) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 4) ^bytes bs))
  (set-extent [this extent]
    (let [extent-offset (unchecked-add-int 4 (get-group-len this))] 
      (.putLong buffer extent-offset extent)))
  (length [this]
    (unchecked-add-int 12 (get-group-len this)))
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
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
  (set-group [this bs]
    (.putShort buffer (unchecked-add-int offset 2) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 4) ^bytes bs))
  (set-extent [this ee]
    (let [[extent1 extent2] ee
          extent1-offset (unchecked-add-int 4 (get-group-len this))
          extent2-offset (unchecked-add-int extent1-offset 8)]
      (.putLong buffer extent1-offset extent1)
      (.putLong buffer extent2-offset extent2)))
  (length [this]
    (unchecked-add-int 20 (get-group-len this)))
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
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
  (get-group-len [this] 0)
  (set-group [this _])
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
  (set-group [this _])
  (set-extent [this _])
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
  (set-group [this _])
  (set-extent [this extent]
    (.putLong buffer 2 extent))
  (length [this] 10)
  (get-group-len [this] 0)
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
  (set-group [this _] 
    nil)
  (set-extent [this [extent1 extent2]]
    (.putLong buffer 2 extent1)
    (.putLong buffer 10 extent2))
  (length [this]
    18)
  (get-group-len [this] 0)
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
