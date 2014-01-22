(ns onyx.coordinator.cluster-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! <! >! tap go]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.system :as s]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]))

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq :eviction-delay 500000}))

(def components (alter-var-root #'system component/start))

(def coordinator (:coordinator components))

(def sync-storage (:sync components))

(def log (:log components))

(def tx-queue (d/tx-report-queue (:conn log)))

(def offer-spy (chan 1000))

(tap (:offer-mult coordinator) offer-spy)

(def catalog
  [{:onyx/name :in
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
    :hornetq/queue-name "out-queue"}])

(def workflow {:in {:inc :out}})

(def n-jobs 15)

(def n-peers 10)

(def tasks-per-job 3)

(doseq [_ (range n-jobs)]
  (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow}))

(doseq [_ (range n-jobs)]
  (<!! offer-spy))

(def peers (take n-peers (repeatedly (fn [] (extensions/create sync-storage :peer)))))

(defn start-peers! [peers]
  (doseq [peer peers]
    (go (try
          (let [payload (extensions/create sync-storage :payload)
                sync-spy (chan 1)
                status-spy (chan 1)]
            (extensions/write-place sync-storage peer payload)
            (extensions/on-change sync-storage payload #(go (>! sync-spy %)))
         
            (>! (:born-peer-ch-head coordinator) peer)

            (loop [payload-node payload]
              (<! sync-spy)

              (let [nodes (:nodes (extensions/read-place sync-storage payload-node))]
                (extensions/on-change sync-storage (:status nodes) #(go (>! status-spy %)))
                (extensions/touch-place sync-storage (:ack nodes))
                (<! status-spy)

                (let [next-payload (extensions/create sync-storage :payload)]
                  (extensions/write-place sync-storage peer next-payload)
                  (extensions/on-change sync-storage next-payload #(go (>! sync-spy %)))
                  (extensions/touch-place sync-storage (:completion nodes))

                  (recur next-payload)))))
          (catch Exception e (prn e))))))

(start-peers! peers)

(testing "All tasks complete"
  (loop []
    (let [db (:db-after (.take tx-queue))
          query '[:find (count ?task) :where [?task :task/complete? true]]
          result (ffirst (d/q query db))]
      (prn result)
      (when-not (= result (* n-jobs tasks-per-job))
        (recur)))))

(def result-db (d/db (:conn log)))

(deftest task-completeness
  (testing "No tasks are left incomplete"
    (let [query '[:find (count ?task) :where [?task :task/complete? false]]
          result (ffirst (d/q query result-db))]
      (is (nil? result)))))

(deftest task-safety
  (testing "No sequential task ever had more than 1 peer"
    (let [query '[:find ?task (count ?peer) :where
                  [?task :task/consumption :sequential]
                  [?peer :peer/task ?task]]
          result (map second (d/q query (d/history result-db)))]
      (is (every? (partial = 1) result)))))

(deftest peer-liveness
  (testing "No peers got 0 tasks"
    (let [query '[:find ?peer :where
                  [?peer :peer/task]]
          result (map first (d/q query (d/history result-db)))]
      (is (= (count result) n-peers)))))

(deftest peer-fairness
  (testing "All peers got a roughly even number of tasks assigned"
    (let [query '[:find ?peer (count ?task) :where
                  [?peer :peer/task ?task]]
          result (map second (d/q query (d/history result-db)))
          mean (/ (* n-jobs tasks-per-job) n-peers)
          confidence 0.5]
      (is (every?
           #(and (<= (- mean (* mean confidence)) %)
                 (>= (+ mean (* mean confidence)) %))
           result)))))

(run-tests 'onyx.coordinator.cluster-test)

(alter-var-root #'system component/stop)

