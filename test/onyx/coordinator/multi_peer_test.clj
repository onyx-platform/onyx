(ns onyx.coordinator.multi-peer-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
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

            payload-node-a-1 (extensions/create sync :payload)
            payload-node-b-1 (extensions/create sync :payload)

            payload-node-a-2 (extensions/create sync :payload)
            payload-node-b-2 (extensions/create sync :payload)

            sync-spy-a (chan 1)
            sync-spy-b (chan 1)
            ack-ch-spy (chan 2)
            offer-ch-spy (chan 10)
            status-spy (chan 2)
            
            catalog [{:onyx/name :in
                      :onyx/direction :input
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :onyx/consumption :sequential
                      :hornetq/queue-name "in-queue"}
                     {:onyx/name :inc
                      :onyx/type :transformer
                      :onyx/consumption :sequential}
                     {:onyx/name :out
                      :onyx/direction :output
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :onyx/consumption :sequential
                      :hornetq/queue-name "out-queue"}]
            workflow {:in {:inc :out}}]

        (tap (:ack-mult coordinator) ack-ch-spy)
        (tap (:offer-mult coordinator) offer-ch-spy)

        (extensions/write-place sync peer-node-a payload-node-a-1)
        (extensions/on-change sync payload-node-a-1 #(>!! sync-spy-a %))

        (extensions/write-place sync peer-node-b payload-node-b-1)
        (extensions/on-change sync payload-node-b-1 #(>!! sync-spy-b %))

        (>!! (:born-peer-ch-head coordinator) peer-node-a)
        (>!! (:born-peer-ch-head coordinator) peer-node-b)

        (<!! offer-ch-spy)
        (<!! offer-ch-spy)

        (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})

        (<!! offer-ch-spy)
        (<!! sync-spy-a)
        (<!! sync-spy-b)

        (testing "Both payloads are received"
          (let [payload-a (extensions/read-place sync payload-node-a-1)
                payload-b (extensions/read-place sync payload-node-b-1)]
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

            (extensions/write-place sync peer-node-a payload-node-a-2)
            (extensions/on-change sync payload-node-a-2 #(>!! sync-spy-a %))

            (extensions/write-place sync peer-node-b payload-node-b-2)
            (extensions/on-change sync payload-node-b-2 #(>!! sync-spy-b %))

            (extensions/touch-place sync (:completion (:nodes payload-a)))
            (extensions/touch-place sync (:completion (:nodes payload-b)))

            (<!! offer-ch-spy)
            (<!! offer-ch-spy)

            (let [[v ch] (alts!! [sync-spy-a sync-spy-b])
                  nodes (:nodes (extensions/read-place sync (:path v)))]
              (extensions/on-change sync (:status nodes) #(>!! status-spy %))
              (extensions/touch-place sync (:ack nodes))

              (<!! ack-ch-spy)
              (<!! status-spy)

              (extensions/touch-place sync (:completion nodes))
              (<!! offer-ch-spy))

            (let [db (d/db (:conn log))]
              
              (testing "All tasks are complete"
                (let [query '[:find (count ?task) :where
                              [?task :task/complete? true]]
                      result (ffirst (d/q query db))]
                  (is (= result 3))))

              (testing "All peers are idle"
                (let [query '[:find (count ?peer) :where
                              [?peer :peer/status :idle]]
                      result (ffirst (d/q query db))]
                  (is (= result 2)))))))))
    
    {:eviction-delay 50000}))

(deftest plan-one-job-four-peers
  (with-system
    (fn [coordinator sync log]
      (let [n 4
            peers (take n (repeatedly (fn [] (extensions/create sync :peer))))
            payloads (take n (repeatedly (fn [] (extensions/create sync :payload))))
            sync-spies (take n (repeatedly (fn [] (chan 1))))
            
            status-spy (chan (* n 5))
            offer-ch-spy (chan (* n 5))
            ack-ch-spy (chan (* n 5))

            catalog [{:onyx/name :in
                      :onyx/direction :input
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :onyx/consumption :sequential
                      :hornetq/queue-name "in-queue"}
                     {:onyx/name :inc
                      :onyx/type :transformer
                      :onyx/consumption :sequential}
                     {:onyx/name :out
                      :onyx/direction :output
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :onyx/consumption :sequential
                      :hornetq/queue-name "out-queue"}]
            workflow {:in {:inc :out}}]

        (tap (:offer-mult coordinator) offer-ch-spy)
        (tap (:ack-mult coordinator) ack-ch-spy)
        
        (doseq [[peer payload sync-spy] (map vector peers payloads sync-spies)]
          (extensions/write-place sync peer payload)
          (extensions/on-change sync payload #(>!! sync-spy %)))

        (doseq [peer peers]
          (>!! (:born-peer-ch-head coordinator) peer))

        (doseq [_ (range n)]
          (<!! offer-ch-spy))

        (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow})
        (<!! offer-ch-spy)

        (alts!! sync-spies)
        (alts!! sync-spies)
        (alts!! sync-spies)

        (let [db (d/db (:conn log))]
          
          (testing "Three peers are :acking"
            (let [query '[:find (count ?peer) :where
                          [?peer :peer/status :acking]]
                  result (ffirst (d/q query db))]
              (is (= result 3))))

          (testing "One peer is idle"
            (let [query '[:find (count ?peer) :where
                          [?peer :peer/status :idle]]
                  result (ffirst (d/q query db))]
              (is (= result 1))))

          (let [query '[:find ?node :where
                        [?peer :peer/status :acking]
                        [?peer :node/payload ?node]]
                payload-nodes (map first (d/q query db))]
            
            (doseq [payload-node payload-nodes]
              (let [payload (extensions/read-place sync payload-node)]
                (extensions/on-change sync (:status (:nodes payload)) #(>!! status-spy %))
                (extensions/touch-place sync (:ack (:nodes payload)))))

            (doseq [_ (range 3)]
              (<!! ack-ch-spy)
              (<!! status-spy))

            (doseq [payload-node payload-nodes]
              (let [payload (extensions/read-place sync payload-node)]
                (extensions/touch-place sync (:completion (:nodes payload)))
                (<!! offer-ch-spy)))))

        (let [db (d/db (:conn log))]
          
          (testing "Four peers are :idle"
            (let [query '[:find (count ?peer) :where
                          [?peer :peer/status :idle]]
                  result (ffirst (d/q query db))]
              (is (= result 4))))

          (testing "All three tasks are complete"
            (let [query '[:find (count ?task) :where
                          [?task :task/complete? true]]
                  result (ffirst (d/q query db))]
              (is (= result 3)))))))
    {:eviction-delay 50000}))

(run-tests 'onyx.coordinator.multi-peer-test)

