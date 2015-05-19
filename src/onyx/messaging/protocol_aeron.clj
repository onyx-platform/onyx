(ns ^:no-doc onyx.messaging.protocol-aeron
  (:require [taoensso.timbre :as timbre]
            [onyx.types :refer [->Leaf]])
  #_(:import [java.util UUID]
           [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
           [uk.co.real_logic.agrona DirectBuffer MutableDirectBuffer]))

(comment
;;;;;;
;; Constants

;; id uuid 
(def ^:const completion-msg-length (int 17))

;; id uuid
(def ^:const retry-msg-length (int 17))

;; id uuid, completion-id uuid, ack-val long
(def ^:const ack-msg-length (int 41))

(def ^:const completion-msg-id (byte 0))
(def ^:const retry-msg-id (byte 1))
(def ^:const ack-msg-id (byte 2))

;; message length without nippy segments
;; id (uuid), acker-id (uuid), completion-id (uuid), ack-val (long)
(def ^:const message-base-length (int 56))

;; messages with 0 messages in it
(def ^:const messages-base-length (int 4))

(defn write-uuid [^MutableDirectBuffer buf offset ^UUID uuid]
  (.putLong buf offset (.getMostSignificantBits uuid))
  (.putLong buf (+ 8 offset) (.getLeastSignificantBits uuid)))

(defn get-uuid [^UnsafeBuffer buf offset]
  (let [msb (.getLong buf offset)
        lsb (.getLong buf (+ 8 offset))]
    (java.util.UUID. msb lsb)))

(defn build-acker-message [^UUID id ^UUID completion-id ^long ack-val]
  (let [buf (UnsafeBuffer. (byte-array ack-msg-length))]
    (.putByte buf 0 ack-msg-id)
    (write-uuid buf 1 id)
    (write-uuid buf 17 completion-id)
    (.putLong buf 33 ack-val)
    buf))

(defn read-message-type [buf offset]
  (.getByte ^UnsafeBuffer buf ^int offset))

(defn read-acker-message [^UnsafeBuffer buf offset]
  (let [id (get-uuid buf offset)
        completion-id (get-uuid buf (+ offset 16))
        ack-val (.getLong buf (+ offset 32))]
    [id completion-id ack-val]))

(defn read-completion [^UnsafeBuffer buf offset]
  (get-uuid buf offset))

(defn read-retry [^UnsafeBuffer buf offset]
  (get-uuid buf offset))

(defn build-completion-msg-buf [id] 
  (let [buf (UnsafeBuffer. (byte-array completion-msg-length))] 
    (.putByte buf 0 completion-msg-id)
    (write-uuid buf 1 id)
    buf))

(defn build-retry-msg-buf [id]
  (let [buf (UnsafeBuffer. (byte-array retry-msg-length))] 
    (.putByte buf 0 retry-msg-id)
    (write-uuid buf 1 id)
    buf))

(defn write-message-meta [^MutableDirectBuffer buf offset {:keys [id acker-id completion-id ack-val]}]
  (write-uuid buf offset id)
  (write-uuid buf (+ offset 16) acker-id)
  (write-uuid buf (+ offset 32) completion-id)
  (.putLong buf (+ offset 48) ack-val))

(defn read-message [^UnsafeBuffer buf offset message]
  (let [id (get-uuid buf offset)
        acker-id (get-uuid buf (+ offset 16))
        completion-id (get-uuid buf (+ offset 32))
        ack-val (.getLong buf (+ offset 48))]
    (->Leaf message id acker-id completion-id ack-val nil nil nil nil)))

(def ^:const message-count-size 4)
(def ^:const payload-size-size 4)

(defn meta-message-offsets [start-pos cnt]
  (reductions + start-pos (repeat cnt message-base-length)))

(defn build-messages-msg-buf [compress-f messages]
  ;; Performance consideration:
  ;; We would rather write to one contiguous byte array or a byteoutputstream
  ;; that returns a byte array. This would require changes to nippy
  (let [message-count (count messages)
        message-payloads ^bytes (compress-f (map :message messages))
        payload-size (alength message-payloads)
        buf-size (+ message-count-size
                    payload-size-size
                    payload-size 
                    (* message-count message-base-length))
        buf (UnsafeBuffer. (byte-array buf-size))] 
    (.putInt buf 0 (int (count messages))) ; number of messages
    (.putInt buf message-count-size payload-size)
    (.putBytes buf 
               (+ message-count-size
                  payload-size-size) 
               message-payloads)
    (let [offset (+ message-count-size payload-size-size payload-size)
          buf-size (reduce (fn [offset msg]
                             (write-message-meta buf offset msg) 
                             (+ message-base-length offset))
                           offset
                           messages)]
      [buf-size buf])))

(defn read-messages-buf [decompress-f ^UnsafeBuffer buf offset length]
  (let [message-count (.getInt buf offset)
        offset (+ offset message-count-size)
        payload-size (.getInt buf offset)
        offset (+ offset payload-size-size)
        ;; Performance consideration:
        ;; We would rather that we didn't need to allocate an additional
        ;; byte array here and have nippy directly read from the buf bytes
        message-payload-bytes (byte-array payload-size)
        _ (.getBytes buf offset message-payload-bytes)
        message-payloads (decompress-f message-payload-bytes)
        offset (+ offset payload-size)]
    (loop [messages (transient []) 
           payloads (seq message-payloads) 
           offset offset]
      (if-let [v (first payloads)] 
        (recur (conj! messages (read-message buf offset v))
               (next payloads)
               (+ offset message-base-length))
        (persistent! messages)))))
)
