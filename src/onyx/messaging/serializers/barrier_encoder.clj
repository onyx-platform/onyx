(ns ^{:no-doc true} onyx.messaging.serializers.barrier-encoder
  (:require [onyx.messaging.serializers.helpers :refer [type->byte]]) 
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PEncoder
  (set-epoch [this epoch])
  (set-opts-map-bytes [this bs])
  (offset [this])
  (length [this])
  (wrap-impl [this buf offset]))

(deftype Encoder [^:unsynchronized-mutable ^UnsafeBuffer buffer ^:unsynchronized-mutable offset]
  PEncoder
  (set-epoch [this epoch]
    (.putLong buffer offset (long epoch))
    this)
  (set-opts-map-bytes [this bs]
    (.putShort buffer (unchecked-add-int offset 8) (short (alength ^bytes bs)))
    (.putBytes buffer (unchecked-add-int offset 10) ^bytes bs)
    this)
  (offset [this] offset)
  (length [this] 
    (unchecked-add-int 10 (.getShort buffer (unchecked-add-int offset 8))))
  (wrap-impl [this buf new-offset] 
    (set! offset new-offset)
    (set! buffer buf)
    (.putShort buffer (unchecked-add-int new-offset 8) (short 0))
    this))

(defn wrap [buffer offset]
  (-> (->Encoder nil nil)
      (wrap-impl buffer offset)))
