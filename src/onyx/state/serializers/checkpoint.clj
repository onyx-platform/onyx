(ns onyx.state.serializers.checkpoint
  "Streaming serializer / deserializer for state checkpoints."
  (:import [org.agrona.concurrent UnsafeBuffer]
           [org.agrona MutableDirectBuffer]
           [org.agrona ExpandableArrayBuffer]))

(def current-schema-version 0)

(defprotocol SEncoder
  (set-schema-version [this version])
  (set-metadata [this bs])
  (set-next-bytes [this bs])
  (encoded-bytes [this])
  (length [this]))

(defprotocol SDecoder
  (get-schema-version [this])
  (get-metadata [this])
  (get-next-bytes [this]))

(deftype StateCheckpointEncoder [^MutableDirectBuffer buffer 
                                 initial-offset
                                 ^:unsynchronized-mutable offset]
  SEncoder
  (set-schema-version [this version]
    (.putInt buffer offset (int version))
    (set! offset (unchecked-add-int offset 4)))
  (set-metadata [this bs]
    (let [len (alength ^bytes bs)
          bytes-offset (unchecked-add-int offset 8)]
      (.putLong buffer offset len)
      (.putBytes buffer bytes-offset ^bytes bs)
      (set! offset (unchecked-add-int bytes-offset len))))
  (set-next-bytes [this bs]
    (let [len (int (alength ^bytes bs))
          bytes-offset (unchecked-add-int offset 4)]
      (.putInt buffer offset len)
      (.putBytes buffer bytes-offset ^bytes bs)
      (set! offset (unchecked-add-int bytes-offset len))))
  (encoded-bytes [this]
    (let [bs (byte-array (- offset initial-offset))] 
      (.getBytes buffer initial-offset bs)
      bs))
  (length [this]
    offset))


(defn empty-checkpoint-encoder []
  (->StateCheckpointEncoder (ExpandableArrayBuffer.) 0 0))

(deftype StateCheckpointDecoder [^MutableDirectBuffer buffer
                                 ^long length
                                 ^:unsynchronized-mutable offset]
  SDecoder
  (get-schema-version [this]
    (let [version (.getInt buffer offset)]
      (set! offset (unchecked-add-int offset 4))
      version))
  (get-metadata [this]
    (let [len (.getLong buffer offset)
          ba (byte-array len)
          bytes-offset (unchecked-add-int offset 8)]
     (.getBytes buffer bytes-offset ba)
     (set! offset (unchecked-add-int bytes-offset len))
     ba))
  (get-next-bytes [this]
    (when (< offset length)
      (let [len (.getInt buffer offset)
            ba (byte-array len)
            bytes-offset (unchecked-add-int offset 4)]
        (.getBytes buffer bytes-offset ba)
        (set! offset (unchecked-add-int bytes-offset len))
        ba))))

(defn new-decoder [^bytes bs]
  (->StateCheckpointDecoder (UnsafeBuffer. bs) (alength bs) 0))

