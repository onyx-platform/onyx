(ns ^:no-doc onyx.messaging.protocol
  (:require [onyx.compression.nippy :refer [compress decompress]]
            [gloss.io :as io]
            [taoensso.timbre :as timbre]
            [gloss.core :as g])
  (:import [java.util UUID]
           [io.netty.buffer ByteBuf Unpooled UnpooledByteBufAllocator PooledByteBufAllocator ByteBufAllocator]))

;; FIXME: CAN USE HEADERLESS?!
;; RAW NIO IMPL
;; TODO should be able to use unsigneds on size fields etc

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

(def type-header-length (int 1)) ; FIXME with tracking uuid
; id uuid 
(def completion-msg-length (int 16))
; id uuid, completion-id uuid, ack-val long
(def ack-msg-length (int 40))
; message length without nippy segments
; id (uuid), acker-id (uuid), completion-id (uuid), ack-val (long), nippy-byte-count (int)
(def message-base-length (int 60))
(def messages-base-length (int 4)) ; messages packet with 0 messages in it

; minimum amount of data we can possibly successfully read from a buf
; is message type + a completion msg
(def min-readable-buf-size (int (+ type-header-length messages-base-length)))

(def completion-payload-length (int (+ completion-msg-length type-header-length)))

(defn build-completion-msg-buf [id] 
  (let [buf ^ByteBuf (byte-buffer completion-payload-length)] 
    (.writeByte buf completion-type-id)
    (write-uuid buf id)
    buf))

(defn read-completion-buf [^ByteBuf buf]
  {:type completion-type-id 
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

(defn read-message-buf [^ByteBuf buf]
  (let [id (take-uuid buf)
        acker-id (take-uuid buf)
        completion-id (take-uuid buf)
        ack-val (.readLong buf)
        message-size (.readInt buf)
        arr (byte-array message-size)
        _ (.readBytes buf arr)
        message (decompress arr)]
    {:id id 
     :acker-id acker-id 
     :completion-id completion-id 
     :ack-val ack-val 
     :message message}))

(defn build-messages-msg-buf [messages] 
  (let [compressed-messages (map (fn [msg]
                                   (update-in msg [:message] compress))
                                 messages)
        ; Probably don't need to count like this any more
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

(defn read-messages-buf [^ByteBuf buf]
  {:type messages-type-id 
   :messages (let [message-count (.readInt buf)]
               (doall (repeatedly message-count #(read-message-buf buf))))})

(defn build-msg-buf [msg]
  (let [t ^byte (:type msg)] 
    (cond 
      (= t messages-type-id) (build-messages-msg-buf (:messages msg))
      (= t ack-type-id) (build-ack-msg-buf (:id msg) (:completion-id msg) (:ack-val msg))
      (= t completion-type-id) (build-completion-msg-buf (:id msg)))))

(defn read-buf [^ByteBuf buf]
  (let [n-left (.readableBytes buf)]
    ; probably don't need this check now with the length decoder
    (if (>= n-left min-readable-buf-size)
      (let [msg-type ^byte (.readByte buf)] 
        (cond (= msg-type messages-type-id) 
              (read-messages-buf buf)
              (= msg-type ack-type-id) 
              (if (<= ack-msg-length (.readableBytes buf))
                (read-ack-buf buf))
              (= msg-type completion-type-id) 
              (if (<= completion-msg-length (.readableBytes buf))
                (read-completion-buf buf))
              :else (throw (Exception. (str "Invalid message type: " msg-type))))))))

;;; GLOSS IMPL
;;;;;; USED BY ALEPH MESSAGING

(defn longs->uuid [[lsbs msbs]]
  (java.util.UUID. msbs lsbs))

(defn uuid->longs [^UUID uuid]
  (vector (.getLeastSignificantBits uuid)
          (.getMostSignificantBits uuid)))

(def char-frame
  (g/compile-frame (g/repeated :byte :prefix :none)))

(def send-frame
  (g/compile-frame 
    {:type :byte
     :messages (g/finite-frame :int32 (g/repeated 
                                        {:id [:int64 :int64]
                                         :acker-id [:int64 :int64]
                                         :completion-id [:int64 :int64]
                                         :message (g/finite-frame :int32 (g/repeated :byte :prefix :none))
                                         :ack-val :int64}                        
                                        :prefix :none))}))

(def ack-frame
  (g/compile-frame {:type :byte
                    :id [:int64 :int64]
                    :completion-id [:int64 :int64]
                    :ack-val :int64}))


(def completion-frame
  (g/compile-frame {:type :byte
                    :id [:int64 :int64]}))

(g/defcodec onyx-codec
  (g/compile-frame
   (g/header
    :ubyte
    (fn [header-byte]
      (cond (= header-byte messages-type-id)
            send-frame
            (= header-byte ack-type-id)
            ack-frame 
            (= header-byte completion-type-id)
            completion-frame))
    :type)))

(defn send-messages->frame [messages]
  {:type messages-type-id
   :messages (map (fn [msg] 
                    (-> msg 
                        (update-in [:id] uuid->longs)
                        (update-in [:acker-id] uuid->longs)
                        (update-in [:completion-id] uuid->longs)
                        (update-in [:message] compress)))
                  messages)})

(defn frame->send-messages [frame]
  (update-in frame
             [:messages]
             (fn [messages] 
               (map (fn [msg]
                      (-> msg 
                          (update-in [:id] longs->uuid)
                          (update-in [:acker-id] longs->uuid)
                          (update-in [:completion-id] longs->uuid)
                          (update-in [:message] (comp decompress
                                                      (partial into-array Byte/TYPE)))))
                    messages))))

(defn ack-msg->frame [msg]
  (-> msg 
      (update-in [:id] uuid->longs)
      (update-in [:completion-id] uuid->longs)
      (assoc :type ack-type-id)))

(defn frame->ack-message [msg]
  (-> msg 
      (update-in [:id] longs->uuid)
      (update-in [:completion-id] longs->uuid)))

(defn completion-msg->frame [msg]
  (-> msg
      (update-in [:id] uuid->longs)
      (assoc :type completion-type-id)))

(defn frame->completion-msg [msg]
  (-> msg 
      (update-in [:id] longs->uuid)))

(defn frame->msg [{:keys [type] :as frame}]
  (cond (= messages-type-id type) 
        (frame->send-messages frame)
        (= ack-type-id type)
        (frame->ack-message frame)
        (= completion-type-id type) 
        (frame->completion-msg frame)))

(def codec-protocol
  (g/compile-frame
    onyx-codec
    identity
    frame->msg))
