(ns onyx.messaging.serializers.segment-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (has-capacity? [this n-bytes])
  (add-message [this bs])
  (offset [this])
  (length [this])
  (segment-count [this])
  (wrap [this offset]))

(deftype Encoder [^UnsafeBuffer buffer 
                  ^:unsynchronized-mutable start-offset
                  ^:unsynchronized-mutable offset]
  PEncoder
  (has-capacity? [this n-bytes]
    (>= (.capacity buffer)
        (+ n-bytes
           offset
           ;; 4 bytes for length
           4)))
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
  (segment-count [this]
    (.getShort buffer start-offset))
  (length [this]
    (- offset start-offset))
  (wrap [this new-offset] 
    (set! start-offset new-offset)
    (.putShort buffer new-offset (int 0))
    (set! offset (+ new-offset 2))
    this))
