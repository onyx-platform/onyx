(ns onyx.messaging.protocol-aeron-test
  (:require [onyx.messaging.protocol-aeron :as protocol]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.types :refer [map->Leaf map->Ack ->Message]]
            [midje.sweet :refer :all]))

(defn read-buf [buf]
  (let [msg-type ^byte (protocol/read-message-type buf 0)] 
    (cond (= msg-type protocol/ack-msg-id)
          (protocol/read-acker-message buf 1)
          (= msg-type protocol/messages-msg-id) 
          (protocol/read-messages-buf decompress buf 1 (dec (.capacity buf)))
          (= msg-type protocol/completion-msg-id) 
          (protocol/read-completion buf 1)
          (= msg-type protocol/retry-msg-id)
          (protocol/read-retry buf 1))))

(let [message (->Message #uuid "08905ef2-c1d3-46f6-b5cb-b563aacc3910"
                         #uuid "11837bd7-2de5-4b62-888d-171c4c47845c")] 
  (fact message =>
        (read-buf (protocol/build-completion-msg-buf (:peer-id message) (:payload message)))))

(let [message (->Message #uuid "7777646b-1bb2-499c-8acb-26cb57c9020a"
                         #uuid "806b9b15-cd18-4558-ae64-73c00923c906")] 
  (fact message =>
        (read-buf (protocol/build-retry-msg-buf (:peer-id message) (:payload message)))))


(let [message (->Message #uuid "3c73fc10-c402-4084-956e-bcc777f3735a"
                (map->Ack {:id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a" 
                           :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                           :ack-val 3323130347050513529}))
      
      ack (:payload message)] 
  (fact message =>
        (read-buf (protocol/build-acker-message (:peer-id message) (:id ack) (:completion-id ack) (:ack-val ack)))))

(let [message (->Message #uuid "ef943b4e-1510-4f4c-a75e-5ad0f1b97569"
                         [(map->Leaf {:id #uuid "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                      :acker-id #uuid "11837bd7-2de5-4b62-888d-171c4c47845c"
                                      :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                      :message {:n 1}
                                      :ack-val 8892143382010058362})
                          (map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                                      :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                                      :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                                      :message {:n 2}
                                      :ack-val 729233382010058362})])]
  (let [[size buf] (protocol/build-messages-msg-buf compress (:peer-id message) (:payload message))]
    (fact message => 
          (read-buf buf))))

(let [message (->Message #uuid "689aec39-7b88-42b5-8419-04857a495af1"
                          [])]
  (let [[size buf] (protocol/build-messages-msg-buf compress (:peer-id message) (:payload message))]
    (fact message => 
          (read-buf buf))))

(let [message (->Message #uuid "c5f475e4-0433-4f60-80da-657e99f44f1f"
                          [(map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                                       :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                                       :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                                       :message {}
                                       :ack-val 729233382010058362})])]
  (let [[size buf] (protocol/build-messages-msg-buf compress (:peer-id message) (:payload message))]
    (fact message => 
          (read-buf buf))))
