(ns onyx.messaging.serializers.segment-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (add-message [this bs])
  (offset [this])
  (length [this])
  (wrap [this offset]))

(deftype Encoder [^UnsafeBuffer buffer 
                  ^:unsynchronized-mutable start-offset
                  ^:unsynchronized-mutable offset]
  PEncoder
  (add-message [this bs]
    (let [len (int (alength ^bytes bs))
          _ (.putInt buffer offset len)
          new-offset (unchecked-add-int offset 4)
          new-count (short (unchecked-add-int (.getShort buffer start-offset) 1))]
      (.putBytes buffer new-offset bs)
      (.putShort buffer start-offset new-count)
      (set! offset (unchecked-add-int new-offset len))
      this))
  (offset [this] offset)
  (length [this]
    (.getShort buffer start-offset))
  (wrap [this new-offset] 
    (set! start-offset new-offset)
    (.putShort buffer new-offset (int 0))
    (set! offset (+ new-offset 2))
    this))
