(ns ^{:no-doc true} onyx.messaging.serializers.local-segment-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (add-batch-ref [this batch-ref])
  (offset [this])
  (length [this])
  (wrap [this offset]))

(deftype Encoder [^UnsafeBuffer buffer 
                  ^:unsynchronized-mutable offset]
  PEncoder
  (add-batch-ref [this batch-ref]
    (.putLong buffer offset batch-ref)
    this)
  (offset [this] offset)
  (length [this]
    8)
  (wrap [this new-offset] 
    (set! offset new-offset)
    this))
