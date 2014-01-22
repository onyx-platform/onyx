(ns onyx.coordinator.coordinator-simulant-tests
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! <! >! tap go]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :refer [tx-ent e hours->msec getx] :as u]
            [datomic.api :as d]
            [onyx.system :as s]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]))

(defn minutes->msec [m]
  (long (* m 60 1000)))

(defn reset-conn
  ([]
     (reset-conn (str "datomic:mem://" (d/squuid))))
  ([uri]
     (d/delete-database uri)
     (d/create-database uri)
     (d/connect uri)))

(defn load-schema
  [conn resource]
  (let [m (-> resource clojure.java.io/resource slurp read-string)]
    (doseq [v (vals m)]
      (doseq [tx v]
        (d/transact conn tx)))))

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (reset-conn sim-uri))

(load-schema sim-conn "simulant/schema.edn")

(load-schema sim-conn "simulant/coordinator-sim.edn")

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

(defn create-test
  [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/stubbed-peer
                           :model/_tests (e model))])
      (tx-ent (:db/id test))))

(defn create-peers [conn test]
  (let [model (-> test :model/_tests u/solo)
        ids (repeatedly (:model/peer-count model) #(d/tempid :test))
        txresult (->> ids
                      (map (fn [id] {:db/id id
                                    :agent/type :agent.type/stable-peer
                                    :test/_agents (e test)}))
                      (d/transact conn))]
    (u/tx-entids @txresult ids)))

(defn generate-peer-executions [test peer]
  (let [model (-> test :model/_tests u/solo)
        limit (:test/duration test)]
    []))

(defn generate-all-executions [test peers]
  (mapcat (partial generate-peer-executions test) peers))

(defmethod sim/create-test :model.type/stubbed-peer
  [conn model test]
  (let [test (create-test conn model test)
        peers (create-peers conn test)]
    (u/transact-batch conn (generate-all-executions test peers) 1000)
    (d/entity (d/db conn) (e test))))

(defmethod sim/create-sim :test.type/stubbed-peer
  [sim-conn test sim]
  (let [model (-> test :model/_tests u/solo)]
    (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
        (tx-ent (:db/id sim)))))

(def model-id (d/tempid :model))

(def coordinator-model-data
  [{:db/id model-id
    :model/type :model.type/stubbed-peer
    :model/peer-count 10}])

(def coordinator-model
  (-> @(d/transact sim-conn coordinator-model-data)
      (tx-ent model-id)))

(def coordinator-test
  (sim/create-test sim-conn coordinator-model
                   {:db/id (d/tempid :test)
                    :test/duration (hours->msec 1)}))

(def coordinator-sim
  (sim/create-sim sim-conn coordinator-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 10}))

(def sim-clock (sim/create-fixed-clock sim-conn coordinator-sim {:clock/multiplier 10000}))

(start-peers! peers)

(def pruns
  (->> #(sim/run-sim-process sim-uri (:db/id coordinator-sim))
       (repeatedly (:sim/processCount coordinator-sim))
       (into [])))

(time
 (mapv (fn [prun] @(:runner prun)) pruns))

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

