(ns ^{:no-doc true} onyx.messaging.serializers.base-decoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (offset [this])
  (length [this])
  (get-type [this])
  (get-replica-version [this])
  (get-payload-length [this])
  (get-dest-id [this])
  (wrap [this buffer offset]))

;; Layout
;; 1 byte: type
;; 8 byte long: replica-version
;; 2 byte short: dest-id
;; 4 byte int: payload length
(deftype Decoder [^:unsynchronized-mutable ^UnsafeBuffer buffer 
                  ^:unsynchronized-mutable ^long offset]
  PDecoder
  (get-type [this]
    (.getByte buffer offset))
  (get-replica-version [this]
    (.getLong buffer (unchecked-add-int offset 1)))
  (get-dest-id [this]
    (.getShort buffer (unchecked-add-int offset 9)))
  (get-payload-length [this]
    (.getInt buffer (unchecked-add-int offset 11)))
  (wrap [this new-buffer new-offset] 
    (set! offset (long new-offset))
    (set! buffer new-buffer)
    this)
  (offset [this] offset)
  (length [this] 15))
