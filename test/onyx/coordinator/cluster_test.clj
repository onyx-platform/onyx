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

(doseq [_ (range 10)]
  (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow}))

(doseq [_ (range 10)]
  (<!! offer-spy))

(def n-peers 10)

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
              (prn peer)

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

(testing "It must complete in under 10 seconds"
  (Thread/sleep 10000))

(let [db (d/db (:conn log))]
  (testing "All 30 tasks completed"
    (let [query '[:find (count ?task) :where [?task :task/complete? true]]
          result (ffirst (d/q query db))]
      (is (= result 30))))

  (testing "No tasks are left incomplete"
    (let [query '[:find (count ?task) :where [?task :task/complete? false]]
          result (ffirst (d/q query db))]
      (is (nil? result)))))

(alter-var-root #'system component/stop)

