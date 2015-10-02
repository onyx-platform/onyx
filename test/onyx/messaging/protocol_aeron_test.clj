(ns onyx.messaging.protocol-aeron-test
  (:require [onyx.messaging.protocol-aeron :as protocol]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.types :refer [map->Leaf map->Ack]]
            [clojure.test :refer [deftest is testing]]))

(defn read-buf [buf]
  (let [msg-type (protocol/read-message-type buf 0)
        peer-id (protocol/read-vpeer-id buf 1)]
    (vector peer-id
            (cond (= msg-type protocol/ack-msg-id)
                  (protocol/read-acker-messages buf 3)
                  (= msg-type protocol/messages-msg-id)
                  (protocol/read-messages-buf decompress buf 3)
                  (= msg-type protocol/completion-msg-id)
                  (protocol/read-completion buf 3)
                  (= msg-type protocol/retry-msg-id)
                  (protocol/read-retry buf 3)))))

(let [peer-id 3834
      message #uuid "11837bd7-2de5-4b62-888d-171c4c47845c"]
  (is (= (vector peer-id message)
         (read-buf (protocol/build-completion-msg-buf peer-id message)))))

(let [peer-id -342
      message #uuid "806b9b15-cd18-4558-ae64-73c00923c906"]
  (is (= (vector peer-id message)
         (read-buf (protocol/build-retry-msg-buf peer-id message)))))


(let [peer-id 32000
      acks [(map->Ack {:id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a"
                       :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                       :ack-val 3323130347050513529})
            (map->Ack {:id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a"
                       :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                       :ack-val 3323130347050513529})]]
  (is (= (vector peer-id (reverse acks))
         (read-buf (protocol/build-acker-messages peer-id acks)))))

(let [peer-id 32765
      message [(map->Leaf {:id #uuid "ac39bc62-8f06-46a0-945e-3a17642a619f"
                           :acker-id #uuid "11837bd7-2de5-4b62-888d-171c4c47845c"
                           :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                           :message {:n 1}
                           :ack-val 8892143382010058362})
               (map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                           :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                           :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                           :message {:n 2}
                           :ack-val 729233382010058362})]]
  (let [buf (protocol/build-messages-msg-buf compress peer-id message)]
    (is (= (vector peer-id message) 
           (read-buf buf)))))

(let [peer-id -32767
      message []]
  (let [buf (protocol/build-messages-msg-buf compress peer-id message)]
    (is (= (vector peer-id message) 
           (read-buf buf)))))

(let [peer-id 0
      message [(map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                           :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                           :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                           :message {}
                           :ack-val 729233382010058362})]]
  (let [buf (protocol/build-messages-msg-buf compress peer-id message)]
    (is (= (vector peer-id message) 
           (read-buf buf)))))
