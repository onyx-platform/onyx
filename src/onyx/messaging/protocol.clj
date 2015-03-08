(ns ^:no-doc onyx.messaging.protocol
  (:require [onyx.compression.nippy :refer [compress decompress]]
            [gloss.io :as io]
            [gloss.core :as g]))

(defn longs->uuid [[lsbs msbs]]
  (java.util.UUID. msbs lsbs))

(defn uuid->longs [uuid]
  (vector (.getLeastSignificantBits uuid)
          (.getMostSignificantBits uuid)))

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

(def send-id (byte 0))
(def ack-id (byte 1))
(def completion-id (byte 2))



(g/defcodec onyx-codec
  (g/compile-frame
   (g/header
    :ubyte
    (fn [header-byte]
      (cond (= header-byte send-id)
            send-frame
            (= header-byte ack-id)
            ack-frame 
            (= header-byte completion-id)
            completion-frame))
    :type)))

(defn send-messages->frame [messages]
  {:type send-id
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
      (assoc :type ack-id)))

(defn frame->ack-message [msg]
  (-> msg 
      (update-in [:id] longs->uuid)
      (update-in [:completion-id] longs->uuid)))

(defn completion-msg->frame [msg]
  (-> msg
      (update-in [:id] uuid->longs)
      (assoc :type completion-id)))

(defn frame->completion-msg [msg]
  (-> msg 
      (update-in [:id] longs->uuid)))

(defn frame->msg [{:keys [type] :as frame}]
  (cond (= send-id type) 
        (frame->send-messages frame)
        (= ack-id type)
        (frame->ack-message frame)
        (= completion-id type) 
        (frame->completion-msg frame)))

(def codec-protocol
  (g/compile-frame
    onyx-codec
    identity
    frame->msg))


