(ns onyx.messaging.protocol-test
  (:require [onyx.messaging.protocol-netty :as protocol]
            [midje.sweet :refer :all]))

;; TODO: fix these tests, implement for Aeron too.

(comment (fact (let [message {:type protocol/completion-type-id
                     :id #uuid "11837bd7-2de5-4b62-888d-171c4c47845c"}] 
        (= message (protocol/read-buf (protocol/build-msg-buf message))))
      => true)

(fact (let [message {:type protocol/ack-type-id
                     :id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a" 
                     :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                     :ack-val 3323130347050513529}] 
        (= message (protocol/read-buf (protocol/build-msg-buf message))))
      => true)

(fact (let [messages {:type protocol/messages-type-id
                      :messages '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                   :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                                   :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                   :message {:n 1}
                                   :ack-val 8892143382010058362}
                                  {:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                                   :message {:n 2}
                                   :ack-val 729233382010058362})}]
        (= messages (protocol/read-buf (protocol/build-msg-buf messages))))
      => true)

(fact (let [messages {:type protocol/messages-type-id
                      :messages '({:id #uuid "010a1688-47ff-4055-8da5-1f02247351e1"
                                   :acker-id #uuid "bf8fd5fc-30fd-424c-af6a-0b32568581a4"
                                   :completion-id #uuid "7ad37c45-ce67-4fd4-8850-f3ec58ede0bf"
                                   :message {}
                                   :ack-val 729233382010058362})}]
        (= messages (protocol/read-buf (protocol/build-msg-buf messages))))
      => true)

;; TODO: investigate why a payload with 0 segments would ever be sent
(fact (let [messages {:type protocol/messages-type-id
                      :messages '()}]
        (= messages (protocol/read-buf (protocol/build-msg-buf messages))))
      => true))
