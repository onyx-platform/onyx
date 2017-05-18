(ns ^{:no-doc true} onyx.messaging.serializers.segment-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (next-value [this])
  (length [this])
  (wrap-impl [this ^UnsafeBuffer buffer offset]))

(deftype Decoder [^bytes bs
                  ^:unsynchronized-mutable ^UnsafeBuffer buffer
                  ^:unsynchronized-mutable n
                  ^:unsynchronized-mutable offset]
  PDecoder
  (wrap-impl [this new-buffer new-offset]
    (set! buffer new-buffer)
    (set! n (- (.getShort ^UnsafeBuffer new-buffer new-offset) (Short/MIN_VALUE)))
    (set! offset (unchecked-add-int new-offset 2))
    this)
  (length [this] n)
  (next-value [this]
    (when (pos? n)
      (let [msg-length (.getInt buffer offset)
            msg-offset (unchecked-add-int offset 4)] 
        (.getBytes buffer msg-offset bs 0 msg-length)
        (set! n (unchecked-add-int n -1))
        (set! offset (unchecked-add-int msg-offset msg-length))
        bs))))

(defn wrap [read-buffer staging-bytes offset]
  (-> (->Decoder staging-bytes nil nil nil)
      (wrap-impl read-buffer offset))) 

(defn read-segments! [decoder vs deserializer-fn]
  (loop []
    (when-let [bs (next-value decoder)]
      (conj! vs (deserializer-fn bs))
      (recur))))
