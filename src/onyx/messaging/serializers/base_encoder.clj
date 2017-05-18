(ns ^{:no-doc true} onyx.messaging.serializers.base-encoder
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (offset [this])
  (length [this])
  (set-type [this type])
  (set-replica-version [this replica-version])
  (set-payload-length [this payload-length])
  (set-dest-id [this dest-id])
  (wrap [this buffer offset]))

;; Layout
;; 1 byte: type
;; 8 byte long: replica-version
;; 2 byte short: dest-id
;; 4 byte int: payload length
(deftype Encoder [^UnsafeBuffer buffer ^long offset]
  PEncoder
  (set-type [this type]
    (.putByte buffer offset (byte type))
    this)
  (set-replica-version [this replica-version]
    (.putLong buffer (unchecked-add-int offset 1)
              (long replica-version))
    this)
  (set-dest-id [this dest-id]
    (.putShort buffer 
               (unchecked-add-int offset 9)
               (short dest-id))
    this)
  (set-payload-length [this payload-length]
    (.putInt buffer 
             (unchecked-add-int offset 11)
             (int payload-length))
    this)
  (wrap [this new-buffer new-offset] 
    (Encoder. new-buffer new-offset))
  (offset [this] offset)
  (length [this] 15))
