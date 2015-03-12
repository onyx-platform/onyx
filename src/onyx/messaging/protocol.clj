(ns ^:no-doc onyx.messaging.protocol
  (:require [onyx.compression.nippy :refer [compress decompress]]
            [gloss.io :as io]
            [gloss.core :as g])
  (:import [java.util UUID]
           [io.netty.buffer ByteBuf Unpooled UnpooledByteBufAllocator ByteBufAllocator]))

;; RAW NIO IMPL

(defn byte-buffer [size]
  (.heapBuffer ^ByteBufAllocator UnpooledByteBufAllocator/DEFAULT size))

(defn write-uuid [^ByteBuf buf ^UUID uuid]
  (.writeLong buf (.getMostSignificantBits uuid))
  (.writeLong buf (.getLeastSignificantBits uuid)))

(defn take-uuid [^ByteBuf buf]
  (java.util.UUID. (.readLong buf)
                   (.readLong buf)))

(def messages-type-id ^byte (byte 0))
(def ack-type-id ^byte (byte 1))
(def completion-type-id ^byte (byte 2))

; msg type byte, id uuid 
(def completion-msg-length 17)
; msg type byte, id uuid, completion-id uuid, ack-val long
(def ack-msg-length 41)
; message length without nippy segments
; id (uuid), acker-id (uuid), completion-id (uuid), ack-val (long), nippy-byte-count (int)
(def message-base-length 60)

; minimum amount of data we can possibly successfully read from a buf
; is message type + a completion msg
(def min-readable-buf-size completion-msg-length)

(defn build-completion-msg-buf [id] 
  (let [buf ^ByteBuf (byte-buffer completion-msg-length)] 
    (.writeByte buf completion-type-id)
    (write-uuid buf id)))

(defn write-message-msg [^ByteBuf buf {:keys [id acker-id completion-id ack-val message]}]
  (write-uuid buf id)
  (write-uuid buf acker-id)
  (write-uuid buf completion-id)
  (.writeLong buf ack-val)
  ; this could probably be a short
  (.writeInt buf (int (count message))) 
  (doall
    (map (fn [b]
           (.writeByte buf b))
         message)))

(defn build-messages-msg-buf [messages] 
  (let [compressed-messages (map (fn [msg]
                                   (update-in msg [:message] compress))
                                 messages)
        buf-size (+ 1 ; message type 
                    4 ; full payload size
                    4 ; message count
                    (* (count messages) message-base-length)
                    ; fressian compressed segments
                    (apply + (map (comp count :message) compressed-messages)))
        buf ^ByteBuf (byte-buffer buf-size)] 
    ; Might have to write total length here so we can tell whether the whole message is in
    (.writeByte buf messages-type-id) ; message type
    ; need the buf-size so we know whether the full payload has arrived
    (.writeInt buf (int buf-size))
    (.writeInt buf (int (count compressed-messages))) ; number of messages
    (doall
      (map (partial write-message-msg buf)
           compressed-messages))
    buf))

(defn build-ack-msg-buf [id completion-id ack-val] 
  (let [buf ^ByteBuf (byte-buffer ack-msg-length)] 
    (.writeByte buf ack-type-id)
    (write-uuid buf id)
    (write-uuid buf completion-id)
    (.writeLong buf ack-val)))

(defn build-msg-buf [msg]
  (let [t ^byte (:type msg)] 
    (cond 
      (= t messages-type-id) (build-messages-msg-buf (:messages msg))
      (= t ack-type-id) (build-ack-msg-buf (:id msg) (:completion-id msg) (:ack-val msg))
      (= t completion-type-id) (build-completion-msg-buf (:id msg)))))

(defn read-message-buf [^ByteBuf buf]
  (let [id (take-uuid buf)
        acker-id (take-uuid buf)
        completion-id (take-uuid buf)
        ack-val (.readLong buf)
        message-size (.readInt buf)
        ; may be slow
        message (decompress (into-array Byte/TYPE (repeatedly message-size #(.readByte buf))))]
    {:id id 
     :acker-id acker-id 
     :completion-id completion-id 
     :ack-val ack-val 
     :message message}))

(defn read-messages-buf [^ByteBuf buf]
  {:type messages-type-id 
   :messages (let [message-count (.readInt buf)]
               (repeatedly message-count #(read-message-buf buf)))})

(defn read-ack-buf [^ByteBuf buf]
  {:type ack-type-id
   :id (take-uuid buf)
   :completion-id (take-uuid buf)
   :ack-val (.readLong buf)})

(defn read-completion-buf [^ByteBuf buf]
  {:type completion-type-id 
   :id (take-uuid buf)})


(defn read-buf [^ByteBuf buf]
  (let [n-left (- (.writerIndex buf) (.readerIndex buf))]
    (if (>= n-left min-readable-buf-size)
      (let [msg-type ^byte (.readByte buf)] 
        (cond (= msg-type messages-type-id) 
              (if (<= (.readInt buf) n-left)
                (read-messages-buf buf))
              (= msg-type ack-type-id) 
              (if (<= ack-msg-length n-left)
                (read-ack-buf buf))
              (= msg-type completion-type-id) 
              (if (<= completion-msg-length n-left)
                (read-completion-buf buf)))))))

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
