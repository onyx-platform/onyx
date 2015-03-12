(ns onyx.messaging.protocol-test
  (:require [onyx.messaging.protocol :as protocol]
            [midje.sweet :refer :all]))

(fact (let [message {:type protocol/completion-id
                     :id #uuid "11837bd7-2de5-4b62-888d-171c4c47845c"}] 
        (= message (protocol/read-buf (protocol/write-msg message))))
      => true)

(fact (let [message {:type protocol/ack-id
                     :id #uuid "e2ba38dd-b523-4e63-ba74-645fb91c231a" 
                     :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                     :ack-val 3323130347050513529}] 
        (= message (protocol/read-buf (protocol/write-msg message))))
      => true)

(fact (let [messages {:type protocol/messages-id
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
        (= messages (protocol/read-buf (protocol/write-msg messages))))
      => true)


;;;;
;;; GLOSS TESTS

(comment 
  ; (io/decode
  ;   onyx-protocol 
  ;   (io/encode onyx-protocol 
  ;              (send-messages->frame '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
  ;                                       :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
  ;                                       :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
  ;                                       :message {:n 1}
  ;                                       :ack-val 7292143382010058362}))))

  (alength (compress '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                        :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                        :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                        :message {:n 1}
                        :ack-val 7292143382010058362}
                       {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                        :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                        :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                        :message {:n 1}
                        :ack-val 7292143382010058362})))

  (reduce + (map (fn [a] (alength (.array a))) 
                 (io/encode onyx-protocol 
                            (send-messages->frame '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                                     :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                                                     :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                                     :message {:n 1}
                                                     :ack-val 7292143382010058362}

                                                    {:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                                     :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                                                     :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                                     :message {:n 1}
                                                     :ack-val 7292143382010058362})))))



  (frame->msg 
    (io/decode
      onyx-protocol
      (io/encode onyx-protocol 
                 (send-messages->frame '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                          :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                                          :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                          :message {:n 1}
                                          :ack-val 7292143382010058362})))))

  (frame->send-messages 
    (io/decode
      onyx-protocol
      (io/encode onyx-protocol 
                 (send-messages->frame '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                          :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                                          :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                          :message {:n 1}
                                          :ack-val 7292143382010058362})))))

  (io/encode onyx-protocol 
             (send-messages->frame '({:id #uuid  "ac39bc62-8f06-46a0-945e-3a17642a619f"
                                      :acker-id #uuid  "11837bd7-2de5-4b62-888d-171c4c47845c"
                                      :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                                      :message {:n 1}
                                      :ack-val 7292143382010058362})))
  
  (io/encode onyx-protocol 
             (completion-msg->frame {:id #uuid "7bdac75d-9fc8-47fb-92b2-73eb885beeea"}))

  (io/encode onyx-protocol
             (ack-msg->frame {:id #uuid  "e2ba38dd-b523-4e63-ba74-645fb91c231a" 
                              :completion-id #uuid  "b57f7be1-f2f9-4d0f-aa02-939b3d48dc23"
                              :ack-val 3323130347050513529})))
