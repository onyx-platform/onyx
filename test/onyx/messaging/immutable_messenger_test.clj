(ns onyx.messaging.immutable-messenger-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx.messaging.messenger :as m]
            [onyx.types :refer [->MonitorEventBytes map->Barrier ->Barrier]]
            [onyx.messaging.atom-messenger :as am]
            [onyx.messaging.immutable-messenger :as im]
            [taoensso.timbre :as timbre :refer [debug info]]))

(defn switch-peer [messenger peer]
  (assoc messenger :id peer))

(defn offer-barriers [messenger]
  (reduce m/offer-barrier (m/next-epoch! messenger) (m/publications messenger)))

(defn offer-barrier-acks [messenger]
  (reduce m/offer-barrier-ack messenger (m/publications messenger)))

(defn process-barriers [messenger] 
  (if (m/all-barriers-seen? messenger)
    (offer-barriers messenger)
    messenger))

(defn ack-barriers [messenger]
  (if (m/all-barriers-seen? messenger)
    (offer-barrier-acks messenger)
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
              (m/set-replica-version! 1)
              (m/add-publication t1-queue-p1)
              (m/add-subscription t2-ack-queue)

              (switch-peer :p2)
              (m/set-replica-version! 1)
              (m/add-publication t1-queue-p2)
              (m/add-subscription t2-ack-queue)

              (switch-peer :p3)
              (m/set-replica-version! 1)
              (m/add-subscription t1-queue-p1)
              (m/add-subscription t1-queue-p2)
              (m/add-publication t2-ack-queue)

              (switch-peer :p4)
              (m/set-replica-version! 1)
              (m/add-subscription t1-queue-p1)
              (m/add-subscription t1-queue-p2)
              (m/add-publication t3-ack-queue)

              (switch-peer :p1)
              ;; Start one epoch higher on the input tasks
              (offer-barriers)
              (m/offer-segments [:m1 :m2] [t1-queue-p1])
              (offer-barriers)
              (m/offer-segments [:m5 :m6] [t1-queue-p1])
              (offer-barriers)

              (switch-peer :p2)
              ;; Start one epoch higher on the input tasks
              (offer-barriers)
              (m/offer-segments [:m3 :m4] [t1-queue-p2])
              ;; don't emit next barrier so that :m5 and :m6 will be blocked
              )
        ms (reductions (fn [m p]
                         (-> m
                             (switch-peer p)
                             ;; make into acking barrier since it's leaf
                             ack-barriers
                             (m/poll)))
                       m
                       [:p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4])
        messages (map :message (mapcat :messages ms))]
    (is (= [:m1 :m3 :m2 :m4] messages))

    ;; Because we've seen all the barriers we can call next epoch
    ;; And continue reading the messages afterwards
    (let [mnext (-> (last ms)
                    (switch-peer :p2)
                    (offer-barriers)
                    (offer-barriers))
          mss (reductions (fn [m p]
                            ;; make into acking barrier since it's leaf
                            (-> m
                                (switch-peer p)
                                ack-barriers
                                (m/poll)))
                          mnext
                          [:p3 :p3 :p3 :p3 :p3 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4 :p3 :p4])]
      (is (= [:m5 :m6] (map :message (mapcat :messages mss))))

      ;; Barriers not seen because barrier was emitted
      (is (not (m/all-barriers-seen? (last mss))))
      ;; Lets emit new barriers and see if all barriers are seen
      (let [m-p4 (-> (last mss)
                     (switch-peer :p1)
                    (offer-barriers)
                     (switch-peer :p2)
                    (offer-barriers)
                     (switch-peer :p4)
                     (m/poll)
                     (m/poll))

            _ (is (m/all-barriers-seen? m-p4))

            m-p1-acks (-> (last mss)
                          (switch-peer :p1)
                          (m/poll-acks)
                          (m/poll-acks)
                          (m/poll-acks)

                          )
            m-p2-acks (-> m-p1-acks 
                          (switch-peer :p2)
                          (m/poll-acks)
                          (m/poll-acks)
                          )
            m-p2-next-acks (-> m-p2-acks
                               (m/unblock-ack-subscriptions!)
                               (m/poll-acks)
                               (m/poll-acks))]
        (is (m/all-acks-seen? m-p1-acks))
        (is (m/all-acks-seen? m-p2-acks))
        (is (m/all-acks-seen? m-p2-next-acks))))))
