(ns onyx.messaging.serializers.segment-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (next-value [this])
  (length [this])
  (wrap [this ^UnsafeBuffer buffer offset]))

(deftype Decoder [^bytes bs
                  ^:unsynchronized-mutable ^UnsafeBuffer buffer
                  ^:unsynchronized-mutable n
                  ^:unsynchronized-mutable offset]
  PDecoder
  (wrap [this new-buffer new-offset]
    (set! buffer new-buffer)
    (set! n (.getShort ^UnsafeBuffer new-buffer new-offset))
    (set! offset (unchecked-add-int new-offset 2))
    this)
  (length [this] n)
  (next-value [this]
    (when (pos? n)
      (let [msg-length (.getInt buffer offset)
            new-offset (unchecked-add-int offset 4)
            _ (.getBytes buffer new-offset bs 0 msg-length)] 
        (set! n (unchecked-add-int n -1))
        (set! offset (unchecked-add-int new-offset msg-length))
        bs))))

(defn read-segments! [decoder vs deserializer-fn]
  (loop []
    (when-let [bs (next-value decoder)]
      (conj! vs (deserializer-fn bs))
      (recur))))
