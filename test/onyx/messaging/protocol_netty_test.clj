(ns onyx.messaging.protocol-netty-test
  (:require [onyx.messaging.protocol-netty :as protocol]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.types :refer [map->Leaf map->Ack]]
            [midje.sweet :refer :all]))

(defn read-buf [decompress-f buf]
  (let [msg-type ^byte (protocol/read-msg-type buf)]
    (cond (= msg-type protocol/messages-type-id)
          (protocol/read-messages-buf decompress-f buf)
          (= msg-type protocol/ack-type-id)
          (protocol/read-acks-buf buf)
          (= msg-type protocol/completion-type-id)
          (protocol/read-completion-buf buf)
          (= msg-type protocol/retry-type-id)
          (protocol/read-retry-buf buf))))

(let [message #uuid "11837bd7-2de5-4b62-888d-171c4c47845c"]
  (fact message =>
        (read-buf decompress
                  (protocol/build-completion-msg-buf message))))

(let [message [(map->Ack {:id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a"
                          :completion-id #uuid "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                          :ack-val 3323130347050513529})]]
  (fact message =>
        (read-buf decompress
                  (protocol/build-acks-msg-buf message))))

(let [messages [(map->Leaf {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                            :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                            :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                            :message {:n 1}
                            :ack-val 8892143382010058362})
                (map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                            :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                            :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                            :message {:n 2}
                            :ack-val 729233382010058362})]]
  (fact messages =>
        (read-buf decompress
                  (protocol/build-messages-msg-buf compress messages))))

(let [messages [(map->Leaf {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                            :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                            :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                            :message {}
                            :ack-val 729233382010058362})]]
  (fact messages =>
        (read-buf decompress
                  (protocol/build-messages-msg-buf compress messages))))

(let [messages '()]
  (fact messages =>
        (read-buf decompress
                  (protocol/build-messages-msg-buf compress messages))))
