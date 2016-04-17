(ns onyx.messaging.immutable-messenger-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as m]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier]]
            [onyx.messaging.immutable-messenger :as im]))

(defn switch-peer [messenger peer]
  (assoc messenger :peer-id peer))

;; Implement barriers where
;; 1. Barrier must be received in order to receive messages
;; 2. Then reads are valid until barrier number is invalidated (via replica number)
;; 3. Implement barrier tracking - read from subscribers that haven't read their next barrier
;; 4. Implement position / ticket differences for reading
;; 5. Implement replica validity - messenger should know what replica it can set messages for and skip past everything for those


;; Need
;; Replayable stream - onyx-seq style for inputs
;; Implement stream rewind and just focus on correctness under these scenarios i.e. unacked stuff

;; B1 - 

(deftest basic-messaging-test
  ;; [:t2 :t1] [:t3 :t1]
  (let [pg (component/start (im/immutable-peer-group {}))
        messenger (im/immutable-messenger pg)
        t1-queue-p1 {:src-peer-id :p1 :dst-task-id :t1}
        t1-queue-p2 {:src-peer-id :p2 :dst-task-id :t1}
        t2-ack-queue {:src-peer-id :p3 :dst-task-id :t1}
        m (-> messenger

              (switch-peer :p1)
              (m/set-replica-version 1)
              (m/register-publication t1-queue-p1)
              (m/register-subscription t2-ack-queue)

              (switch-peer :p2)
              (m/set-replica-version 1)
              (m/register-publication t1-queue-p2)
              (m/register-subscription t2-ack-queue)

              (switch-peer :p3)
              (m/set-replica-version 1)
              (m/register-subscription t1-queue-p1)
              (m/register-subscription t1-queue-p2)
              (m/register-publication t2-ack-queue)

              (switch-peer :p1)
              (m/emit-barrier)
              (m/send-messages [:m1 :m2] [t1-queue-p1])
              (m/next-epoch)
              (m/emit-barrier)

              (switch-peer :p2)
              (m/emit-barrier)
              (m/send-messages [:m3 :m4] [t1-queue-p2])
              (m/next-epoch)
              (m/emit-barrier)
              (m/send-messages [:m5 :m6] [t1-queue-p2]))

        ms (reductions (fn [m _]
                         (m/receive-messages m))
                       (switch-peer m :p3)
                       (range 20))
        messages (keep :message ms)]
    (is (= [:m1 :m3 :m2 :m4] messages))
    (is (m/all-barriers-seen? (last ms)))

    ;; Because we've seen all the barriers we can call next epoch
    ;; And continue reading the messages afterwards
    (let [mnext (-> (last ms)
                    (m/ack-barrier)
                    (m/next-epoch)
                    (switch-peer :p3))
          mss (reductions (fn [m _]
                            (m/receive-messages m))
                          mnext
                          (range 20))]
      (is (= [:m5 :m6] (keep :message mss)))
      (is (not (m/all-barriers-seen? (last mss))))
      (let [m-p1-acks (-> (last mss)
                          (switch-peer :p1)
                          (m/receive-acks))
            m-p2-acks (-> m-p1-acks 
                          (switch-peer :p2)
                          (m/receive-acks))
            m-p2-next-acks (m/receive-acks m-p2-acks)]
        (is (not (empty? (:acks m-p1-acks))))
        (is (not (empty? (:acks m-p2-acks))))
        (is (empty? (:acks m-p2-next-acks)))))))
