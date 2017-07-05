(ns ^{:no-doc true} onyx.state.serializers.windowing-key-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (wrap-impl [this bs])
  (get-type [this])
  (get-state-idx [this])
  (get-group [this])
  (get-group-len [this])
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
  (get-group-len [this]
    (.getShort buffer (unchecked-add-int offset 2)))
  (get-group [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 2)))] 
      (.getBytes buffer (unchecked-add-int offset 4) bs)
      bs))
  (length [this]
    (unchecked-add-int 4 (get-group-len this))))

(deftype GroupedNoExtentDecoder [^UnsafeBuffer buffer offset]
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
  (get-extent [this] nil)
  (length [this]
    (unchecked-add-int 12 (get-group-len this))))

(deftype GroupedLongExtentDecoder [^UnsafeBuffer buffer offset]
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
  (get-extent [this]
    (.getLong buffer (unchecked-add-int 4 (get-group-len this))))
  (length [this]
    (unchecked-add-int 12 (get-group-len this))))

(deftype GroupedLongLongExtentDecoder [^UnsafeBuffer buffer offset]
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
  (get-extent [this]
    (let [extent1-offset (unchecked-add-int 4 (get-group-len this))] 
      [(.getLong buffer extent1-offset)
       (.getLong buffer (unchecked-add-int 8 extent1-offset))]))
  (length [this]
    (unchecked-add-int 20 (get-group-len this))))

(deftype UngroupedTriggerDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-extent [this]
    (throw (Exception. "not implemented")))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-len [this]
    0)
  (get-group [this])
  (length [this]
    4))

(deftype UngroupedNoExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-len [this] 0)
  (get-group [this] nil)
  (get-extent [this] nil)
  (length [this] 2))

(deftype UngroupedLongExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-len [this] 0)
  (get-group [this] nil)
  (get-extent [this]
    (.getLong buffer 2))
  (length [this] 10))

(deftype UngroupedLongLongExtentDecoder [^UnsafeBuffer buffer offset]
  PDecoder
  (wrap-impl [this bs]
    (.wrap buffer ^bytes bs))
  (get-state-idx [this]
    (.getShort buffer offset))
  (get-group-len [this]
    0)
  (get-group [this] nil)
  (get-extent [this]
    [(.getLong buffer 2)
     (.getLong buffer 10)])
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
