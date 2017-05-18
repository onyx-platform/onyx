(ns ^{:no-doc true} onyx.messaging.serializers.ready-reply-decoder
  (:require [onyx.messaging.serializers.helpers :refer [byte->type]])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-epoch [this])
  (get-src-peer-id [this])
  (get-dst-peer-id [this])
  (get-session-id [this]))

(deftype Decoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-src-peer-id [this]
    [(byte->type (.getByte buffer offset))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 1))
                      (.getLong buffer (unchecked-add-int offset 9)))])
  (get-dst-peer-id [this]
    [(byte->type (.getByte buffer (unchecked-add-int offset 17)))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 18))
                      (.getLong buffer (unchecked-add-int offset 26)))])
  (get-session-id [this]
    (.getLong buffer (unchecked-add-int offset 34))))

(defn wrap [buffer offset]
  (->Decoder buffer offset))
