(ns ^:no-doc onyx.messaging.protocol-aeron
  (:require [taoensso.timbre :as timbre]
            [onyx.types :refer [->Leaf ->Ack]])
  (:import [java.util UUID]
           [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
           [uk.co.real_logic.agrona DirectBuffer MutableDirectBuffer]))

;comment
;;;;;;
;; Constants

;; id uuid
(def ^:const completion-msg-length (long 33))

;; id uuid
(def ^:const retry-msg-length (long 33))

;; id uuid, completion-id uuid, ack-val long
(def ^:const ack-msg-length (long 40))

(def ^:const completion-msg-id (byte 0))
(def ^:const retry-msg-id (byte 1))
(def ^:const ack-msg-id (byte 2))
(def ^:const messages-msg-id (byte 3))

(def ^:const short-size (long 2))

;; message length without nippy segments
;; id (uuid), acker-id (uuid), completion-id (uuid), ack-val (long)
(def ^:const message-base-length (long 56))

;; messages with 0 messages in it
(def ^:const messages-base-length (long 4))

(def ^:const message-count-size (long 4))
(def ^:const payload-size-size (long 4))
(def ^:const messages-header-size (long (+ message-count-size payload-size-size short-size 1)))

(defn read-vpeer-id [^UnsafeBuffer buf ^long offset]
  (.getShort buf offset))

(defn write-vpeer-id [^UnsafeBuffer buf ^long offset peer-id]
  (.putShort buf offset (short peer-id)))

(defn write-uuid [^MutableDirectBuffer buf ^long offset ^UUID uuid]
  (.putLong buf offset (.getMostSignificantBits uuid))
  (.putLong buf (unchecked-add 8 offset) (.getLeastSignificantBits uuid)))

(defn get-uuid [^UnsafeBuffer buf ^long offset]
  (let [msb (.getLong buf offset)
        lsb (.getLong buf (unchecked-add 8 offset))]
    (java.util.UUID. msb lsb)))

(defn build-acker-messages [peer-id messages]
  (let [message-count (count messages)
        buffer-size (+ 7 (* message-count ack-msg-length))
        buf (UnsafeBuffer. (byte-array buffer-size))] 
    (.putByte buf 0 ack-msg-id)
    (write-vpeer-id buf 1 peer-id)
    (.putInt buf 3 message-count)
    (reduce (fn [offset msg]
              (write-uuid buf offset (:id msg))
              (write-uuid buf (unchecked-add offset 16) (:completion-id msg))
              (.putLong buf (unchecked-add offset 32) (:ack-val msg))
              (unchecked-add offset ack-msg-length)) 
            7
            messages)
    (list buffer-size buf)))

(defn read-message-type [buf offset]
  (.getByte ^UnsafeBuffer buf ^long offset))

(defn read-acker-messages [^UnsafeBuffer buf ^long offset]
  (let [message-count (.getInt buf offset)]
    (loop [messages (list)
           cnt 0
           offset (unchecked-add offset 4)]
      (if (= cnt message-count)
        messages
        (let [id (get-uuid buf offset)
              completion-id (get-uuid buf (unchecked-add offset 16))
              ack-val (.getLong buf (unchecked-add offset 32))]
          (recur (conj messages (->Ack id completion-id ack-val nil))
                 (inc cnt)
                 (unchecked-add offset 40))))))) 

(defn read-completion [^UnsafeBuffer buf ^long offset]
  (get-uuid buf offset))

(defn read-retry [^UnsafeBuffer buf ^long offset]
  (get-uuid buf offset))

(defn build-completion-msg-buf [peer-id id]
  (let [buf (UnsafeBuffer. (byte-array completion-msg-length))]
    (.putByte buf 0 completion-msg-id)
    (write-vpeer-id buf 1 peer-id)
    (write-uuid buf 3 id)
    buf))

(defn build-retry-msg-buf [peer-id id]
  (let [buf (UnsafeBuffer. (byte-array retry-msg-length))]
    (.putByte buf 0 retry-msg-id)
    (write-vpeer-id buf 1 peer-id)
    (write-uuid buf 3 id)
    buf))

(defn write-message-meta [^MutableDirectBuffer buf ^long offset msg]
  (write-uuid buf offset (:id msg))
  (write-uuid buf (unchecked-add offset 16) (:acker-id msg))
  (write-uuid buf (unchecked-add offset 32) (:completion-id msg))
  (.putLong buf (unchecked-add offset 48) (:ack-val msg)))

(defn read-message [^UnsafeBuffer buf ^long offset message]
  (let [id (get-uuid buf offset)
        acker-id (get-uuid buf (unchecked-add offset 16))
        completion-id (get-uuid buf (unchecked-add offset 32))
        ack-val (.getLong buf (unchecked-add offset 48))]
    (->Leaf message id acker-id completion-id ack-val nil nil)))

(defn build-messages-msg-buf [compress-f peer-id messages]
  ;; Performance consideration:
  ;; We would rather write to one contiguous byte array or a byteoutputstream
  ;; that returns a byte array. This would require changes to nippy
  (let [message-count (count messages)
        message-payloads ^bytes (compress-f (map :message messages))
        payload-size (alength message-payloads)
        buf-size (unchecked-add messages-header-size
                                (unchecked-add payload-size
                                               (* message-count message-base-length)))
        buf (UnsafeBuffer. (byte-array buf-size))
        _ (.putByte buf 0 messages-msg-id)
        _ (write-vpeer-id buf 1 peer-id)
        _ (.putInt buf 3 message-count) ; number of messages
        _ (.putInt buf 7 payload-size)
        _ (.putBytes buf messages-header-size message-payloads)
        offset (unchecked-add messages-header-size payload-size)
        buf-size (reduce (fn [offset msg]
                           (write-message-meta buf offset msg)
                           (unchecked-add message-base-length ^long offset))
                         offset
                         messages)]
    (list buf-size buf)))

(defn read-messages-buf [decompress-f ^UnsafeBuffer buf ^long offset length]
  (let [message-count (.getInt buf offset)
        offset (unchecked-add offset message-count-size)
        payload-size (.getInt buf offset)
        offset (unchecked-add offset payload-size-size)
        ;; Performance consideration:
        ;; We would rather that we didn't need to allocate an additional
        ;; byte array here and have nippy directly read from the buf bytes
        message-payload-bytes (byte-array payload-size)
        _ (.getBytes buf offset message-payload-bytes)
        message-payloads (decompress-f message-payload-bytes)
        offset (unchecked-add offset payload-size)
        segments (loop [messages (transient [])
                        payloads (seq message-payloads)
                        offset offset]
                   (if-let [v (first payloads)]
                     (recur (conj! messages (read-message buf offset v))
                            (next payloads)
                            (unchecked-add offset message-base-length))
                     (persistent! messages)))]
    segments))
