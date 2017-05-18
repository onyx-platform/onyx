(ns ^{:no-doc true} onyx.messaging.serializers.heartbeat-encoder
  (:require [onyx.messaging.serializers.helpers :refer [type->byte]])         
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-epoch [this epoch])
  (set-dst-peer-id [this peer-id])
  (set-src-peer-id [this peer-id])
  (set-session-id [this session-id])
  (set-opts-map-bytes [this bs])
  (offset [this])
  (length [this])
  (wrap-impl [this buf offset]))

(deftype Encoder [^:unsynchronized-mutable ^UnsafeBuffer buffer ^:unsynchronized-mutable offset]
  PEncoder
  (set-epoch [this epoch]
    (.putLong buffer offset (long epoch))
    this)
  (set-src-peer-id [this [peer-type peer-id]]
    (.putByte buffer (unchecked-add-int offset 8) (type->byte peer-type))
    (.putLong buffer (unchecked-add-int offset 9) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 17) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-dst-peer-id [this [peer-type peer-id]]
    (.putByte buffer (unchecked-add-int offset 25) (type->byte peer-type))
    (.putLong buffer (unchecked-add-int offset 26) (.getMostSignificantBits ^java.util.UUID peer-id))
    (.putLong buffer (unchecked-add-int offset 34) (.getLeastSignificantBits ^java.util.UUID peer-id))
    this)
  (set-session-id [this session-id]
    (.putLong buffer (unchecked-add-int offset 42) (long session-id))
    this)
  (set-opts-map-bytes [this bs]
    (.putShort buffer (unchecked-add-int offset 50) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 52) ^bytes bs)
    this)
  (offset [this] offset)
  (length [this] 
    (unchecked-add-int 52 (.getShort buffer (unchecked-add-int offset 50))))
  (wrap-impl [this buf new-offset] 
    (set! offset new-offset)
    (set! buffer buf)
    (.putShort buffer (unchecked-add-int new-offset 50) (short 0))
    this))

(defn wrap [buffer offset]
  (-> (->Encoder nil nil)
      (wrap-impl buffer offset)))
