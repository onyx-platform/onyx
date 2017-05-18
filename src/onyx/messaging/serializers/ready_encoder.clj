(ns ^{:no-doc true} onyx.messaging.serializers.ready-encoder
  (:require [onyx.messaging.serializers.helpers :refer [type->byte]])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-src-peer-id [this peer-id])
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
  (offset [this] offset)
  (length [this]
    17)
  (wrap-impl [this buf new-offset] 
    (set! offset new-offset)
    (set! buffer buf)
    this))

(defn wrap [buffer offset]
  (-> (->Encoder nil nil)
      (wrap-impl buffer offset)))
