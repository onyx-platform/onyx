(ns onyx.coordinator.cluster-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! <! >! tap go timeout]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.system :as s]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.coordinator.sim-test-utils :refer [with-system]]))

(defn start-peers! [peers coordinator sync-storage]
  (doseq [peer peers]
    (go (try
          (let [payload (extensions/create sync-storage :payload)
                sync-spy (chan 1)
                status-spy (chan 1)]
            (extensions/write-place sync-storage peer payload)
            (extensions/on-change sync-storage payload #(go (>! sync-spy %)))
         
            (>! (:born-peer-ch-head coordinator) peer)

            (loop [payload-node payload]
              (<! (timeout (rand-nth (range 800))))
              (<! sync-spy)

              (let [nodes (:nodes (extensions/read-place sync-storage payload-node))]
                (extensions/on-change sync-storage (:status nodes) #(go (>! status-spy %)))
                (extensions/touch-place sync-storage (:ack nodes))
                (<! status-spy)
                (<! (timeout (rand-nth (range 1200))))

                (let [next-payload (extensions/create sync-storage :payload)]
                  (extensions/write-place sync-storage peer next-payload)
                  (extensions/on-change sync-storage next-payload #(go (>! sync-spy %)))
                  (extensions/touch-place sync-storage (:completion nodes))

                  (recur next-payload)))))
          (catch Exception e (prn e))))))

(defn run-cluster [n-jobs n-peers]
  (with-system
    (fn [coordinator sync-storage log]
      (let [tx-queue (d/tx-report-queue (:conn log))
            offer-spy (chan 10000)

            catalog [{:onyx/name :in
                      :onyx/direction :input
                      :onyx/consumption :sequential
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :hornetq/queue-name "in-queue"}
                     {:onyx/name :inc
                      :onyx/type :transformer
                      :onyx/consumption :sequential}
                     {:onyx/name :out
                      :onyx/direction :output
                      :onyx/consumption :sequential
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :hornetq/queue-name "out-queue"}]
            workflow {:in {:inc :out}}
            tasks-per-job 3]

        (tap (:offer-mult coordinator) offer-spy)

        (doseq [_ (range n-jobs)]
          (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow}))

        (doseq [_ (range n-jobs)]
          (<!! offer-spy))

        (let [peers (take n-peers (repeatedly (fn [] (extensions/create sync-storage :peer))))]
          (start-peers! peers coordinator sync-storage)
          (testing "All tasks complete"
            
            (loop []
              (let [db (:db-after (.take tx-queue))
                    query '[:find (count ?task) :where [?task :task/complete? true]]
                    result (ffirst (d/q query db))]
                (prn result)
                (when-not (= result (* n-jobs tasks-per-job))
                  (recur)))))

          (d/db (:conn log)))))
    {:eviction-delay 500000}))

(defn task-completeness [result-db]
  (let [query '[:find (count ?task) :where [?task :task/complete? false]]
        result (ffirst (d/q query result-db))]
    (is (nil? result))))

(defn task-safety [result-db]
  (let [query '[:find ?task (count ?peer) :where
                [?task :task/consumption :sequential]
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))]
    (is (every? (partial = 1) result))))

(defn peer-liveness [result-db n-peers]
  (let [query '[:find ?peer :where
                [?peer :peer/task]]
        result (map first (d/q query (d/history result-db)))]
    (is (= (count result) n-peers))))

(defn peer-fairness [result-db n-peers n-jobs tasks-per-job]
  (let [query '[:find ?peer (count ?task) :where
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))
        mean (/ (* n-jobs tasks-per-job) n-peers)
        confidence 0.5]
    (is (every?
         #(and (<= (- mean (* mean confidence)) %)
               (>= (+ mean (* mean confidence)) %))
         result))))

(defn concurrency-liveness [result-db n-jobs tasks-per-job]
  (let [query '[:find ?task (count ?peer) :where
                [?task :task/consumption :concurrent]
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))]
    (is (>= (count (filter (partial < 1) result))
            (/ (* n-jobs tasks-per-job) 0.25)))))

(deftest small-even-cluster
  (let [n-jobs 15
        n-peers 10
        tasks-per-job 3
        result-db (run-cluster n-jobs n-peers)]
    
    (testing "No tasks are left incomplete"
      (task-completeness result-db))

    (testing "No sequential task ever had more than 1 peer"
      (task-safety result-db))

    (testing "No peers got 0 tasks"
      (peer-liveness result-db n-peers))

    (testing "All peers got a roughly even number of tasks assigned"
      (peer-fairness result-db n-peers n-jobs tasks-per-job))))

(deftest small-cluster-many-jobs
  (let [n-jobs 150
        n-peers 10
        tasks-per-job 3
        result-db (run-cluster n-jobs n-peers)]
    
    (testing "No tasks are left incomplete"
      (task-completeness result-db))

    (testing "No sequential task ever had more than 1 peer"
      (task-safety result-db))

    (testing "No peers got 0 tasks"
      (peer-liveness result-db n-peers))

    (testing "All peers got a roughly even number of tasks assigned"
      (peer-fairness result-db n-peers n-jobs tasks-per-job))))

(deftest big-cluster-few-jobs
  (let [n-jobs 10
        n-peers 200
        tasks-per-job 3
        result-db (run-cluster n-jobs n-peers)]
    
    (testing "No tasks are left incomplete"
      (task-completeness result-db))

    (testing "No sequential task ever had more than 1 peer"
      (task-safety result-db))))

(run-tests 'onyx.coordinator.cluster-test)


