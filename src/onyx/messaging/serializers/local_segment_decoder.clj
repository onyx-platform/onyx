(ns ^{:no-doc true} onyx.messaging.serializers.local-segment-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-batch-ref [this])
  (length [this])
  (wrap-impl [this ^UnsafeBuffer buffer offset]))

(deftype Decoder [^:unsynchronized-mutable ^UnsafeBuffer buffer
                  ^:unsynchronized-mutable offset]
  PDecoder
  (wrap-impl [this new-buffer new-offset]
    (set! buffer new-buffer)
    (set! offset new-offset)
    this)
  (length [this] 8)
  (get-batch-ref [this]
    (.getLong buffer offset)))

(defn wrap [read-buffer offset]
  (-> (->Decoder nil nil)
      (wrap-impl read-buffer offset))) 
