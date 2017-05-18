(ns ^{:no-doc true} onyx.messaging.serializers.heartbeat-decoder
  (:require [onyx.messaging.serializers.helpers :refer [byte->type]])
  (:import [org.agrona.concurrent UnsafeBuffer]))

(defprotocol PDecoder
  (get-epoch [this])
  (get-src-peer-id [this])
  (get-dst-peer-id [this])
  (get-session-id [this])
  (get-opts-map-bytes [this]))

(deftype Decoder [^UnsafeBuffer buffer offset]
  PDecoder
  (get-epoch [this]
    (.getLong buffer offset))
  (get-src-peer-id [this]
    [(byte->type (.getByte buffer (unchecked-add-int offset 8)))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 9))
                      (.getLong buffer (unchecked-add-int offset 17)))])
  (get-dst-peer-id [this]
    [(byte->type (.getByte buffer (unchecked-add-int offset 25)))
     (java.util.UUID. (.getLong buffer (unchecked-add-int offset 26))
                      (.getLong buffer (unchecked-add-int offset 34)))])
  (get-session-id [this]
    (.getLong buffer (unchecked-add-int offset 42)))
  (get-opts-map-bytes [this]
    (let [bs (byte-array (.getShort buffer (unchecked-add-int offset 50)))] 
      (.getBytes buffer (unchecked-add-int offset 52) bs)
      bs)))

(defn wrap [buffer offset]
  (->Decoder buffer offset))
