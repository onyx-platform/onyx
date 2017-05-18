(ns ^{:no-doc true} onyx.messaging.serializers.ready-decoder
  (:require [onyx.messaging.serializers.helpers :refer [byte->type]])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-src-peer-id [this]))

(deftype Decoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-src-peer-id [this]
    [(byte->type (.getByte buffer offset))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 1))
                      (.getLong buffer (unchecked-add-int offset 9)))]))

(defn wrap [buffer offset]
  (->Decoder buffer offset))
