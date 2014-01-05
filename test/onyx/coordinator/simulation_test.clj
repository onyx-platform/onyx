(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.system :as s]
            [onyx.util :as u]))

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq}))

(defn with-system [f]
  (let [components (alter-var-root #'system component/start)
        coordinator (:coordinator components)
        sync (:sync components)
        log (:log components)]
    (try
      (f coordinator sync log)
      (finally
       (alter-var-root #'system component/stop)))))

(deftest new-peer
  (with-system
    (fn [coordinator sync log]
      (let [peer (extensions/create sync :peer)
            offer-ch-spy (chan 1)]
        (tap (:offer-mult coordinator) offer-ch-spy)
        (>!! (:born-peer-ch-head coordinator) peer)
        (<!! offer-ch-spy)

        (testing "There is one peer"
            (let [query '[:find ?p :where [?e :peer/place ?p]]
                  result (d/q query (d/db (:conn log)))]
              (is (= (count result) 1))
              (is (= (ffirst result) peer))))))))

(deftest peer-joins-and-dies
  (with-system
    (fn [coordinator sync log]
      (let [peer (extensions/create sync :peer)
            offer-ch-spy (chan 1)
            evict-ch-spy (chan 1)]
        (tap (:offer-mult coordinator) offer-ch-spy)
        (tap (:evict-mult coordinator) evict-ch-spy)
        (>!! (:born-peer-ch-head coordinator) peer)
        (<!! offer-ch-spy)
        (extensions/delete sync peer)
        (<!! evict-ch-spy)

        (testing "There are no peers"
            (let [query '[:find ?p :where [?e :peer/place ?p]]
                  result (d/q query (d/db (:conn log)))]
              (is (zero? (count result)))))))))

(deftest plan-one-job-no-peers
  (with-system
    (fn [coordinator sync log]
      (let [catalog [{:onyx/name :in
                      :onyx/direction :input
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :hornetq/queue-name "in-queue"}
                     {:onyx/name :inc
                      :onyx/type :transformer}
                     {:onyx/name :out
                      :onyx/direction :output
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :hornetq/queue-name "out-queue"}]
            workflow {:in {:inc :out}}
            offer-ch-spy (chan 1)]
        (tap (:offer-mult coordinator) offer-ch-spy)
        (>!! (:planning-ch-head coordinator)
             {:catalog catalog :workflow workflow})

        (let [job-id (<!! offer-ch-spy)
              db (d/db (:conn log))]

          (testing "There is one job"
              (let [query '[:find ?j :in $ ?id :where [?j :job/id ?id]]
                    result (d/q query db job-id)]
                (is (= (count result) 1))))

          (testing "There are three tasks")
          (let [query '[:find ?n :where [?t :task/name ?n]]
                result (d/q query db)]
            (is (= result #{[:in] [:inc] [:out]})))

          (testing ":in's ingress queue is preset"
            (let [query '[:find ?qs :where
                          [?t :task/name :in]
                          [?t :task/ingress-queues ?qs]]
                  result (d/q query db)]
              (is (= result #{["in-queue"]}))))

          (testing ":out's ingress queue is preset"
            (let [query '[:find ?qs :where
                          [?t :task/name :out]
                          [?t :task/ingress-queues ?qs]]
                  result (d/q query db)]
              (is (= result #{["out-queue"]}))))

          (testing ":out has no egress queue"
            (let [query '[:find ?qs :where
                          [?t :task/name :out]
                          [?t :task/egress-queues ?qs]]
                  result (d/q query db)]
              (is (empty? result))))

          (testing ":inc's ingress queue is :in's egress queue")
          (let [in-query '[:find ?qs :where
                           [?t :task/name :in]
                           [?t :task/egress-queues ?qs]]
                inc-query '[:find ?qs :where
                            [?t :task/name :inc]
                            [?t :task/ingress-queues ?qs]]]
            (is (= (d/q in-query db) (d/q inc-query db))))

          (testing ":out's ingess queue is :inc's egress queue"
            (let [inc-query '[:find ?qs :where
                              [?t :task/name :inc]
                              [?t :task/egress-queues ?qs]]
                  out-query '[:find ?qs :where
                              [?t :task/name :out]
                              [?t :task/ingress-queues ?qs]]]
              (is (= (d/q inc-query db) (d/q out-query db))))))))))

(deftest plan-one-job-one-peer
  (with-system
    (fn [coordinator sync log]
      (let [peer (extensions/create sync :peer)
            offer-ch-spy (chan 1)
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
        (tap (:offer-mult coordinator) offer-ch-spy)
        (>!! (:born-peer-ch-head coordinator) peer)
        (>!! (:planning-ch-head coordinator)
             {:catalog catalog :workflow workflow})
        (<!! offer-ch-spy)
        (let [db (d/db (:conn log))]
          (<!! (clojure.core.async/timeout 1000)))))))

(run-tests 'onyx.coordinator.simulation-test)

