(ns onyx.peer.simulant.linear-cluster-sim-test
  (:require [midje.sweet :refer :all]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [datomic.api :as d]
            [taoensso.timbre :refer [info]]
            [onyx.peer.simulant.sim-test-utils :as sim-utils]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id id
    :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def env (onyx.api/start-env env-config))

(def cluster (atom []))

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def n-messages 60000)

(def batch-size 1320)

(def echo 1000)

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(defn create-birth [executor t]
  [[{:db/id (d/tempid :test)
     :agent/_actions (u/e executor)
     :action/atTime t
     :action/type :action.type/register-linear-peer}]])

(defn create-death [executor t]
  [[{:db/id (d/tempid :test)
     :agent/_actions (u/e executor)
     :action/atTime t
     :action/type :action.type/unregister-linear-peer}]])

(defn generate-linear-scaling-data [test executor]
  (let [model (-> test :model/_tests first)
        limit (:test/duration test)
        rate (:model/peer-rate model)
        peers (:model/peek-peers model)
        gap (:model/silence-gap model)
        births (mapcat (partial create-birth executor)
                       (range 0 (* peers rate) rate))
        deaths (mapcat (partial create-death executor)
                       (range (+ (* peers rate) gap)
                              (+ (* (* peers rate) 2) gap)
                              rate))]
    (concat births deaths)))

(defn create-linear-cluster-test [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/linear-cluster
                           :model/_tests (u/e model))])
      (u/tx-ent (:db/id test))))

(defn create-executor [conn test]
  (let [tid (d/tempid :test)
        result @(d/transact conn
                            [{:db/id tid
                              :agent/type :agent.type/executor
                              :test/_agents (u/e test)}])]
    (d/resolve-tempid (d/db conn) (:tempids result) tid)))

(defmethod sim/create-test :model.type/linear-cluster
  [conn model test]
  (let [test (create-linear-cluster-test conn model test)
        executor (create-executor conn test)]
    (u/transact-batch conn (generate-linear-scaling-data test executor) 1000)
    (d/entity (d/db conn) (u/e test))))

(defmethod sim/create-sim :test.type/linear-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (sim-utils/reset-conn sim-uri))

(sim-utils/load-schema sim-conn "simulant/schema.edn")

(sim-utils/load-schema sim-conn "simulant/peer-sim.edn")

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :hornetq/queue-name in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.simulant.linear-cluster-sim-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def workflow [[:in :inc] [:inc :out]])

(hq-util/create-queue! hq-config in-queue)
(hq-util/create-queue! hq-config out-queue)

(hq-util/write-and-cap! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) echo)

(onyx.api/submit-job
 peer-config
 {:catalog catalog
  :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def linear-model-id (d/tempid :model))

(def linear-cluster-model-data
  [{:db/id linear-model-id
    :model/type :model.type/linear-cluster
    :model/peek-peers 5
    :model/peer-rate 200
    :model/silence-gap 5000}])

(def linear-cluster-model
  (-> @(d/transact sim-conn linear-cluster-model-data)
      (u/tx-ent linear-model-id)))

(defmethod sim/perform-action :action.type/register-linear-peer
  [action process]
  (let [peers (onyx.api/start-peers! 1 peer-config)]
    (info (count @cluster) "in the cluster")
    (swap! cluster concat peers)))

(defmethod sim/perform-action :action.type/unregister-linear-peer
  [action process]
  (let [peer (first @cluster)]
    (swap! cluster rest)
    (info (count @cluster) "left in the cluster")
    (onyx.api/shutdown-peer peer)))

(def linear-cluster-test
  (sim/create-test sim-conn
                   linear-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration 60000}))

(def linear-cluster-sim
  (sim/create-sim sim-conn
                  linear-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

(sim/create-fixed-clock sim-conn linear-cluster-sim {:clock/multiplier 1})

(sim/create-action-log sim-conn linear-cluster-sim)

(doseq [n (range 3)]
  (swap! cluster concat (onyx.api/start-peers! 1 peer-config)))

(def pruns
  (->> #(sim/run-sim-process sim-uri (:db/id linear-cluster-sim))
       (repeatedly (:sim/processCount linear-cluster-sim))
       (into [])))

(def results (hq-util/consume-queue! hq-config out-queue echo))

(doseq [prun pruns] (future-cancel (:runner prun)))

(doseq [peer @cluster]
  (onyx.api/shutdown-peer peer))

(onyx.api/shutdown-env env)

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

