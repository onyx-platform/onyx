(ns ^:no-doc onyx.messaging.protocol-netty
    (:require [taoensso.timbre :as timbre])
    (:import [java.util UUID]
             [io.netty.buffer ByteBuf Unpooled UnpooledByteBufAllocator
              PooledByteBufAllocator ByteBufAllocator]))

;;;;;;
;; helpers
(defn byte-buffer [size]
  (.heapBuffer ^ByteBufAllocator PooledByteBufAllocator/DEFAULT size))

(defn write-uuid [^ByteBuf buf ^UUID uuid]
  (.writeLong buf (.getMostSignificantBits uuid))
  (.writeLong buf (.getLeastSignificantBits uuid)))

(defn take-uuid [^ByteBuf buf]
  (let [msb (.readLong buf)
        lsb (.readLong buf)] 
    (java.util.UUID. msb lsb)))

;;;;;;
;; Constants
(def messages-type-id ^byte (byte 0))
(def ack-type-id ^byte (byte 1))
(def completion-type-id ^byte (byte 2))
(def retry-type-id ^byte (byte 3))

(def type-header-length (int 1))

; id uuid 
(def completion-msg-length (int 16))

; id uuid 
(def retry-msg-length (int 16))

; id uuid, completion-id uuid, ack-val long
(def ack-msg-length (int 40))

; message length without nippy segments
; id (uuid), acker-id (uuid), completion-id (uuid), ack-val (long), nippy-byte-count (int)
(def message-base-length (int 60))

; messages with 0 messages in it
(def messages-base-length (int 4))

(def completion-payload-length (int (+ completion-msg-length type-header-length)))

(def retry-payload-length (int (+ retry-msg-length type-header-length)))

(defn build-completion-msg-buf [id] 
  (let [buf ^ByteBuf (byte-buffer completion-payload-length)] 
    (.writeByte buf completion-type-id)
    (write-uuid buf id)
    buf))

(defn build-retry-msg-buf [id] 
  (let [buf ^ByteBuf (byte-buffer retry-payload-length)] 
    (.writeByte buf retry-type-id)
    (write-uuid buf id)
    buf))

(defn read-completion-buf [^ByteBuf buf]
  {:type completion-type-id 
   :id (take-uuid buf)})

(defn read-retry-buf [^ByteBuf buf]
  {:type retry-type-id 
   :id (take-uuid buf)})

(def ack-payload-length ^int (+ ack-msg-length type-header-length))

(defn build-ack-msg-buf [id completion-id ack-val] 
  (let [^ByteBuf buf (byte-buffer ack-payload-length)] 
    (.writeByte buf ack-type-id)
    (write-uuid buf id)
    (write-uuid buf completion-id)
    (.writeLong buf ack-val)
    buf))

(defn read-ack-buf [^ByteBuf buf]
  (let [id (take-uuid buf)
        completion-id (take-uuid buf)
        ack-val (.readLong buf)]
    {:type ack-type-id
     :id id 
     :completion-id completion-id
     :ack-val ack-val}))

(defn write-message-msg [^ByteBuf buf {:keys [id acker-id completion-id ack-val message]}]
  (write-uuid buf id)
  (write-uuid buf acker-id)
  (write-uuid buf completion-id)
  (.writeLong buf ack-val)
  ; this could probably be a short
  (.writeInt buf (alength ^bytes message)) 
  (.writeBytes buf ^bytes message))

(defn read-message-buf [decompress-f ^ByteBuf buf]
  (let [id (take-uuid buf)
        acker-id (take-uuid buf)
        completion-id (take-uuid buf)
        ack-val (.readLong buf)
        message-size (.readInt buf)
        arr (byte-array message-size)
        _ (.readBytes buf arr)
        message (decompress-f arr)]
    {:id id 
     :acker-id acker-id 
     :completion-id completion-id 
     :ack-val ack-val 
     :message message}))

(defn build-messages-msg-buf [compress-f messages] 
  (let [compressed-messages (map (fn [msg]
                                   (update-in msg [:message] compress-f))
                                 messages)
        buf-size (+ type-header-length
                    (+ 4 ; message count int
                       (* (count messages) message-base-length)
                       ; nippy compressed segments
                       (apply + (map (fn [m]
                                       (alength ^bytes (:message m)))  
                                     compressed-messages))))
        buf ^ByteBuf (byte-buffer buf-size)] 
    (.writeByte buf messages-type-id) ; message type header
    (.writeInt buf (int (count compressed-messages))) ; number of messages
    (doseq [msg compressed-messages]
      (write-message-msg buf msg))
    buf))

(defn read-messages-buf [decompress-f ^ByteBuf buf]
  {:type messages-type-id 
   :messages (let [message-count (.readInt buf)]
               (doall (repeatedly message-count #(read-message-buf decompress-f buf))))})

(defn build-msg-buf [msg]
  (let [t ^byte (:type msg)] 
    (cond 
      (= t messages-type-id) (build-messages-msg-buf (:messages msg))
      (= t ack-type-id) (build-ack-msg-buf (:id msg) (:completion-id msg) (:ack-val msg))
      (= t completion-type-id) (build-completion-msg-buf (:id msg))
      (= t retry-type-id) (build-retry-msg-buf (:id msg)))))

(defn read-buf [decompress-f ^ByteBuf buf]
  (let [msg-type ^byte (.readByte buf)] 
    (cond (= msg-type messages-type-id) 
          (read-messages-buf decompress-f buf)
          (= msg-type ack-type-id) 
          (read-ack-buf buf)
          (= msg-type completion-type-id) 
          (read-completion-buf buf)
          (= msg-type retry-type-id)
          (read-retry-buf buf)
          :else (throw (Exception. (str "Invalid message type: " msg-type))))))

