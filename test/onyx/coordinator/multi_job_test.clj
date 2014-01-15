(ns onyx.coordinator.multi-job-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap alts!! >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.system :as s]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(deftest plan-two-jobs-two-peers
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
            
            catalog-a [{:onyx/name :in-a
                        :onyx/direction :input
                        :onyx/type :queue
                        :onyx/medium :hornetq
                        :onyx/consumption :sequential
                        :hornetq/queue-name "in-queue"}
                       {:onyx/name :inc-a
                        :onyx/type :transformer
                        :onyx/consumption :sequential}
                       {:onyx/name :out-a
                        :onyx/direction :output
                        :onyx/type :queue
                        :onyx/medium :hornetq
                        :onyx/consumption :sequential
                        :hornetq/queue-name "out-queue"}]
            
            catalog-b [{:onyx/name :in-b
                        :onyx/direction :input
                        :onyx/type :queue
                        :onyx/medium :hornetq
                        :onyx/consumption :sequential
                        :hornetq/queue-name "in-queue"}
                       {:onyx/name :inc-b
                        :onyx/type :transformer
                        :onyx/consumption :sequential}
                       {:onyx/name :out-b
                        :onyx/direction :output
                        :onyx/type :queue
                        :onyx/medium :hornetq
                        :onyx/consumption :sequential
                        :hornetq/queue-name "out-queue"}]
            
            workflow-a {:in-a {:inc-a :out-a}}
            workflow-b {:in-b {:inc-b :out-b}}]

        (tap (:ack-mult coordinator) ack-ch-spy)
        (tap (:offer-mult coordinator) offer-ch-spy)

        (extensions/write-place sync peer-node-a payload-node-a-1)
        (extensions/on-change sync payload-node-a-1 #(>!! sync-spy-a %))

        (extensions/write-place sync peer-node-b payload-node-b-1)
        (extensions/on-change sync payload-node-b-1 #(>!! sync-spy-b %))

        (>!! (:born-peer-ch-head coordinator) peer-node-a)
        (<!! offer-ch-spy)

        (>!! (:planning-ch-head coordinator) {:catalog catalog-a :workflow workflow-a})
        (<!! offer-ch-spy)
        
        (>!! (:born-peer-ch-head coordinator) peer-node-b)
        (<!! offer-ch-spy)

        (<!! sync-spy-a)
        (<!! sync-spy-b)

        (let [payload-a (extensions/read-place sync payload-node-a-1)
              payload-b (extensions/read-place sync payload-node-b-1)]

          (let [db (d/db (:conn log))]
            
            (testing "Payload A is for job A"
              (let [query '[:find ?job :in $ ?task ?catalog :where
                            [?job :job/task ?task]
                            [?job :job/catalog ?catalog]]
                    result (d/q query db (:db/id (:task payload-a)) (pr-str catalog-a))
                    jobs (map first result)]
                (is (= (count jobs) 1))))

            (testing "Payload B is for job A"
              (let [query '[:find ?job :in $ ?task ?catalog :where
                            [?job :job/task ?task]
                            [?job :job/catalog ?catalog]]
                    result (d/q query db (:db/id (:task payload-b)) (pr-str catalog-a))
                    jobs (map first result)]
                (is (= (count jobs) 1)))))

          (extensions/on-change sync (:status (:nodes payload-a)) #(>!! status-spy %))
          (extensions/on-change sync (:status (:nodes payload-b)) #(>!! status-spy %))

          (extensions/touch-place sync (:ack (:nodes payload-a)))
          (extensions/touch-place sync (:ack (:nodes payload-b)))

          (<!! ack-ch-spy)
          (<!! ack-ch-spy)

          (<!! status-spy)
          (<!! status-spy)

          (>!! (:planning-ch-head coordinator) {:catalog catalog-b :workflow workflow-b})
          (<!! offer-ch-spy)

          (extensions/write-place sync peer-node-a payload-node-a-2)
          (extensions/on-change sync payload-node-a-2 #(>!! sync-spy-a %))
          (extensions/touch-place sync (:completion (:nodes payload-a))))

        (<!! offer-ch-spy)
        (<!! sync-spy-a)

        (let [payload-a (extensions/read-place sync payload-node-a-2)]
          (testing "Payload A is for job B"
            (let [db (d/db (:conn log))
                  query '[:find ?job :in $ ?task ?catalog :where
                          [?job :job/task ?task]
                          [?job :job/catalog ?catalog]]
                  result (d/q query db (:db/id (:task payload-a)) (pr-str catalog-b))
                  jobs (map first result)]
              (is (= (count jobs) 1))))

          (extensions/on-change sync (:status (:nodes payload-a)) #(>!! status-spy %))
          (extensions/touch-place sync (:ack (:nodes payload-a)))

          (<!! ack-ch-spy)
          (<!! status-spy))

        (let [payload-b (extensions/read-place sync payload-node-b-1)]
          (extensions/write-place sync peer-node-b payload-node-b-2)
          (extensions/on-change sync payload-node-b-2 #(>!! sync-spy-b %))
          (extensions/touch-place sync (:completion (:nodes payload-b)))

          (<!! offer-ch-spy)
          (<!! sync-spy-b))

        (let [payload-b (extensions/read-place sync payload-node-b-2)]
          (testing "Payload B is for job A"
            (let [db (d/db (:conn log))
                  query '[:find ?job :in $ ?task ?catalog :where
                          [?job :job/task ?task]
                          [?job :job/catalog ?catalog]]
                  result (d/q query db (:db/id (:task payload-b)) (pr-str catalog-a))
                  jobs (map first result)]
              (is (= (count jobs) 1)))))))
    
    {:eviction-delay 50000}))

(run-tests 'onyx.coordinator.multi-job-test)

