(ns onyx.coordinator.multi-peer-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.system :as s]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))


(deftest plan-one-job-two-peers
  (with-system
    (fn [coordinator sync log]
      (let [peer-node-a (extensions/create sync :peer)
            peer-node-b (extensions/create sync :peer)

            payload-node-a (extensions/create sync :payload)
            payload-node-b (extensions/create sync :payload)

            sync-spy-a (chan 1)
            sync-spy-b (chan 1)
            ack-ch-spy (chan 2)
            offer-ch-spy (chan 1)
            status-spy (chan 2)
            
            catalog [{:onyx/name :in
                      :onyx/direction :input
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :onyx/consumption :concurrent
                      :hornetq/queue-name "in-queue"}
                     {:onyx/name :inc
                      :onyx/type :transformer
                      :onyx/consumption :sequential}
                     {:onyx/name :out
                      :onyx/direction :output
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :onyx/consumption :concurrent
                      :hornetq/queue-name "out-queue"}]
            workflow {:in {:inc :out}}]

        (tap (:ack-mult coordinator) ack-ch-spy)
        (tap (:offer-mult coordinator) offer-ch-spy)

        (extensions/write-place sync peer-node-a payload-node-a)
        (extensions/on-change sync payload-node-a #(>!! sync-spy-a %))

        (extensions/write-place sync peer-node-b payload-node-b)
        (extensions/on-change sync payload-node-b #(>!! sync-spy-b %))

        (>!! (:born-peer-ch-head coordinator) peer-node-a)
        (>!! (:born-peer-ch-head coordinator) peer-node-b)

        (<!! offer-ch-spy)
        (<!! offer-ch-spy)

        (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})

        (<!! sync-spy-a)
        (<!! sync-spy-b)

        (testing "Both payloads are received"
          (let [payload-a (extensions/read-place sync payload-node-a)
                payload-b (extensions/read-place sync payload-node-b)]
            (is (not (nil? payload-a)))
            (is (not (nil? payload-b)))
            (is (not= payload-a payload-b))

            (extensions/on-change sync (:status (:nodes payload-a)) #(>!! status-spy %))
            (extensions/on-change sync (:status (:nodes payload-b)) #(>!! status-spy %))

            (extensions/touch-place sync (:ack (:nodes payload-a)))
            (extensions/touch-place sync (:ack (:nodes payload-b)))

            (<!! ack-ch-spy)
            (<!! ack-ch-spy)

            (<!! status-spy)
            (<!! status-spy)

            (extensions/touch-place sync (:completion (:nodes payload-a)))
            (extensions/touch-place sync (:completion (:nodes payload-b)))
            
            ))))
    
    {:eviction-delay 50000}))

(run-tests 'onyx.coordinator.multi-peer-test)
