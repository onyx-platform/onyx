(ns onyx.messaging.serialize
  (:require [onyx.compression.nippy :refer [messaging-compress messaging-decompress]]
            [onyx.messaging.serializers.base-encoder :as base-encoder])
  (:import [org.agrona.concurrent UnsafeBuffer]
           [onyx.messaging.serializers.base_decoder.Decoder]))


(def message-id ^:const (byte 0))
(def barrier-id ^:const (byte 1))
(def heartbeat-id ^:const (byte 2))
(def ready-id ^:const (byte 3))
(def ready-reply-id ^:const (byte 4))

; (defn message [replica-version short-id payload]
;   {:type message-id :replica-version replica-version :short-id short-id :payload payload})

(defn barrier [replica-version epoch short-id]
  {:type barrier-id :replica-version replica-version :epoch epoch :short-id short-id})

;; should be able to get rid of src-peer-id via short-id
(defn ready [replica-version src-peer-id short-id]
  {:type ready-id :replica-version replica-version :src-peer-id src-peer-id :short-id short-id})

(defn ready-reply [replica-version src-peer-id dst-peer-id session-id short-id]
  {:type ready-reply-id :replica-version replica-version :src-peer-id src-peer-id 
   :dst-peer-id dst-peer-id :session-id session-id :short-id short-id})

(defn heartbeat [replica-version epoch src-peer-id dst-peer-id session-id short-id]
  {:type heartbeat-id :replica-version replica-version :epoch epoch 
   :src-peer-id src-peer-id :dst-peer-id dst-peer-id :session-id session-id
   :short-id short-id})

(defn serialize ^UnsafeBuffer [msg]
  (let [bs ^bytes (messaging-compress msg)
        msg-length (alength bs)
        enc (base-encoder/->Encoder nil 0)
        buf (UnsafeBuffer. (byte-array (+ msg-length (base-encoder/length enc))))
        enc (-> enc
                (base-encoder/wrap buf 0)
                (base-encoder/set-type (:type msg))
                (base-encoder/set-replica-version (:replica-version msg))
                (base-encoder/set-dest-id (:short-id msg))
                (base-encoder/set-payload-length msg-length))]
    (.putBytes buf (base-encoder/length enc) bs)
    buf))  

(defn deserialize [^UnsafeBuffer buf offset length]
  (let [bs (byte-array length)] 
    (.getBytes buf offset bs)
    (messaging-decompress bs)))
