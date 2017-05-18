(ns ^{:no-doc true} onyx.messaging.serializers.ready-reply-encoder
  (:require [onyx.messaging.serializers.helpers :refer [type->byte]]) 
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-dst-peer-id [this peer-id])
  (set-src-peer-id [this peer-id])
  (set-session-id [this session-id])
  (offset [this])
  (length [this])
  (wrap-impl [this buf offset]))


(deftype Encoder [^:unsynchronized-mutable ^UnsafeBuffer buffer ^:unsynchronized-mutable offset]
  PEncoder
  (set-src-peer-id [this [peer-type peer-id]]
    (.putByte buffer offset (type->byte peer-type))
    (.putLong buffer (unchecked-add-int offset 1) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 9) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-dst-peer-id [this [peer-type peer-id]]
    (.putByte buffer (unchecked-add-int offset 17) (type->byte peer-type))
    (.putLong buffer (unchecked-add-int offset 18) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 26) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-session-id [this session-id]
    (.putLong buffer (unchecked-add-int offset 34) (long session-id))
    this)
  (offset [this] offset)
  (length [this] 42)
  (wrap-impl [this buf new-offset] 
    (set! offset new-offset)
    (set! buffer buf)
    this))

(defn wrap [buffer offset]
  (-> (->Encoder nil nil)
      (wrap-impl buffer offset)))
