(ns onyx.messaging.immutable-messenger-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as m]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier]]
            [onyx.messaging.atom-messenger :as am]
            [onyx.messaging.immutable-messenger :as im]
            [taoensso.timbre :as timbre :refer [debug info]]))

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

(defn process-barriers [messenger] 
  (if (m/all-barriers-seen? messenger)
    (m/emit-barrier messenger)
    messenger))

(defn ack-barriers [messenger]
  (if (m/all-barriers-seen? messenger)
    (m/ack-barrier messenger)
    messenger))

(deftest basic-messaging-test
  ;; [:t2 :t1] [:t3 :t1]
  (let [pg (component/start (im/immutable-peer-group {}))
        messenger (im/immutable-messenger pg) 
        t1-queue-p1 {:src-peer-id :p1 :dst-task-id :t1}
        t1-queue-p2 {:src-peer-id :p2 :dst-task-id :t1}
        t2-ack-queue {:src-peer-id :p3 :dst-task-id :t1}
        t3-ack-queue {:src-peer-id :p4 :dst-task-id :t1}
        m (-> messenger

              (switch-peer :p1)
              (m/set-replica-version 1)
              (m/add-publication t1-queue-p1)
              (m/add-subscription t2-ack-queue)

              (switch-peer :p2)
              (m/set-replica-version 1)
              (m/add-publication t1-queue-p2)
              (m/add-subscription t2-ack-queue)

              (switch-peer :p3)
              (m/set-replica-version 1)
              (m/add-subscription t1-queue-p1)
              (m/add-subscription t1-queue-p2)
              (m/add-publication t2-ack-queue)

              (switch-peer :p4)
              (m/set-replica-version 1)
              (m/add-subscription t1-queue-p1)
              (m/add-subscription t1-queue-p2)
              (m/add-publication t3-ack-queue)

              (switch-peer :p1)
              ;; Start one epoch higher on the input tasks
              (m/emit-barrier)
              (m/send-messages [:m1 :m2] [t1-queue-p1])
              (m/emit-barrier)
              (m/send-messages [:m5 :m6] [t1-queue-p1])
              (m/emit-barrier)

              (switch-peer :p2)
              ;; Start one epoch higher on the input tasks
              (m/emit-barrier)
              (m/send-messages [:m3 :m4] [t1-queue-p2])
              ;; don't emit next barrier so that :m5 and :m6 will be blocked
              
              )
        ms (reductions (fn [m p]
                         (-> m
                             (switch-peer p)
                             ;; make into acking barrier since it's leaf
                             ack-barriers
                             m/receive-messages))
                       m
                       [:p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4])
        messages (map :message (mapcat :messages ms))]
    (is (= [:m1 :m3 :m2 :m4] messages))

    ;; Because we've seen all the barriers we can call next epoch
    ;; And continue reading the messages afterwards
    (let [mnext (-> (last ms)
                    (switch-peer :p2)
                    (m/emit-barrier)
                    (m/emit-barrier))
          mss (reductions (fn [m p]
                            ;; make into acking barrier since it's leaf
                            (-> m
                                (switch-peer p)
                                ack-barriers
                                m/receive-messages))
                          mnext
                          [:p3 :p3 :p3 :p3 :p3 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4])]
      (is (= [:m5 :m6] (map :message (mapcat :messages mss))))

      ;; Barriers not seen because barrier was emitted
      (is (not (m/all-barriers-seen? (last mss))))
      ;; Lets emit new barriers and see if all barriers are seen
      (let [m-p4 (-> (last mss)
                     (switch-peer :p1)
                     (m/emit-barrier)
                     (switch-peer :p2)
                     (m/emit-barrier)
                     (switch-peer :p4)
                     (m/receive-messages)
                     (m/receive-messages))]
        (is (m/all-barriers-seen? m-p4))

        ;(is (m/all-barriers-seen? (m/receive-messages (switch-peer (last mss) :p4))))
        )

      (let [m-p1-acks (-> (last mss)
                          (switch-peer :p1)
                          (m/receive-acks)
                          (m/receive-acks)
                          (m/receive-acks)
                          
                          )
            m-p2-acks (-> m-p1-acks 
                          (switch-peer :p2)
                          (m/receive-acks)
                          (m/receive-acks)
                          )
            m-p2-next-acks (-> m-p2-acks
                               (m/flush-acks)
                               (m/receive-acks)
                               (m/receive-acks)
                               )]
        (println "All acks seen"
             (m/all-acks-seen? m-p1-acks)    
                 )
        (is (m/all-acks-seen? m-p1-acks))
        (is (m/all-acks-seen? m-p2-acks))
        (is (m/all-acks-seen? m-p2-next-acks))))))


;; Mutable version test - uses atom to replicate results in above test
#_(deftest atom-messaging-test
  ;; [:t2 :t1] [:t3 :t1]
  (let [pg (component/start (am/atom-peer-group {}))
        m-p1 (component/start (-> (am/atom-messenger)
                                  (assoc :peer {:peer-group {:messaging-group pg}
                                                :id :p1})))
        m-p2 (component/start (-> (am/atom-messenger)
                                  (assoc :peer {:peer-group {:messaging-group pg}
                                                :id :p2})))
        m-p3 (component/start (-> (am/atom-messenger)
                                  (assoc :peer {:peer-group {:messaging-group pg}
                                                :id :p3})))
        t1-queue-p1 {:src-peer-id :p1 :dst-task-id :t1}
        t1-queue-p2 {:src-peer-id :p2 :dst-task-id :t1}
        t2-ack-queue {:src-peer-id :p3 :dst-task-id :t1}
        _ (-> m-p1
              (m/set-replica-version 1)
              (m/add-publication t1-queue-p1)
              (m/add-subscription t2-ack-queue))

        _ (-> m-p2
              (m/set-replica-version 1)
              (m/add-publication t1-queue-p2)
              (m/add-subscription t2-ack-queue))

        m (-> m-p3
              (m/set-replica-version 1)
              (m/add-subscription t1-queue-p1)
              (m/add-subscription t1-queue-p2)
              (m/add-publication t2-ack-queue))

        _ (-> m-p1 
              (m/next-epoch)
              (m/emit-barrier)
              (m/send-messages [:m1 :m2] [t1-queue-p1])
              (m/next-epoch)
              (m/emit-barrier))

        _ (-> m-p2
              (m/next-epoch)
              (m/emit-barrier)
              (m/send-messages [:m3 :m4] [t1-queue-p2])
              (m/next-epoch)
              (m/emit-barrier)
              (m/send-messages [:m5 :m6] [t1-queue-p2])) 
        _ (process-barriers m-p3)
        messages (map :message (mapcat (fn [_] (m/receive-messages m-p3)) (range 20)))]
    (is (= [:m1 :m3 :m2 :m4] messages))
    (is (m/all-barriers-seen? m))

    ;; Because we've seen all the barriers we can call next epoch
    ;; And continue reading the messages afterwards
    (let [mnext (-> m-p3
                    (m/ack-barrier)
                    (m/next-epoch))
          messages2 (map :message (mapcat (fn [_] (m/receive-messages m-p3)) (range 20)))]
      (is (= [:m5 :m6] messages2))
      (is (not (m/all-barriers-seen? mnext)))
      (let [m-p1-acks (m/receive-acks m-p1)
            m-p2-acks (m/receive-acks m-p2)
            m-p2-next-acks (m/receive-acks m-p2)]
        (is (not (empty? m-p1-acks)))
        (is (not (empty? m-p2-acks)))
        (is (empty? m-p2-next-acks))
        ;; Late joiner :p4 on same queues at p3 should not obtain any messages
        (let [m-p4 (component/start (-> (am/atom-messenger)
                                        (assoc :peer {:peer-group {:messaging-group pg}
                                                      :id :p4})))
              m (-> m-p4
                    (m/set-replica-version 1)
                    (m/add-subscription t1-queue-p1)
                    (m/add-subscription t1-queue-p2)
                    (m/add-publication t2-ack-queue))]

          (is (empty? (remove nil? (mapcat (fn [_] (m/receive-messages m-p4)) (range 20))))))))))
