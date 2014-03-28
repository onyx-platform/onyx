(ns onyx.peer.simulant.linear-cluster-sim
  (:require [midje.sweet :refer :all]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [datomic.api :as d]
            [onyx.coordinator.sim-test-utils :as sim-utils]
            [onyx.peer.hornetq-util :as hq-util]
            [onyx.api]))

(def cluster (atom []))

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def n-messages 150)

(def batch-size 50)

(def echo 1)

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(hq-util/write-and-cap! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) echo)

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

(sim-utils/load-schema sim-conn "simulant/coordinator-sim.edn")

(def id (str (java.util.UUID/randomUUID)))

(def timeout 500)

(def datomic (str "datomic:mem://" id))

(def coord-opts
  {:datomic-uri datomic
   :hornetq-host hornetq-host
   :hornetq-port hornetq-port
   :zk-addr "127.0.0.1:2181"
   :onyx-id id
   :revoke-delay 2000})

(def peer-opts {:hornetq-host hornetq-host
                :hornetq-port hornetq-port
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

(def catalog
  [{:onyx/name :in
    :onyx/direction :input
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :hornetq/batch-size batch-size
    :hornetq/timeout timeout}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.simulant.linear-cluster-sim/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size
    :onyx/timeout timeout}

   {:onyx/name :out
    :onyx/direction :output
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size
    :onyx/timeout timeout}])

(def workflow {:in {:inc :out}})

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def linear-model-id (d/tempid :model))

(def linear-cluster-model-data
  [{:db/id linear-model-id
    :model/type :model.type/linear-cluster
    :model/n-peers 1
    :model/peek-peers 1
    :model/peer-rate 200
    :model/silence-gap 5000}])

(def linear-cluster-model
  (-> @(d/transact sim-conn linear-cluster-model-data)
      (u/tx-ent linear-model-id)))

(defmethod sim/perform-action :action.type/register-linear-peer
  [action process]
  (let [peer (first (onyx.api/start-peers conn 1 peer-opts))]
    (swap! cluster conj peer)))

(defmethod sim/perform-action :action.type/unregister-linear-peer
  [action process]
  (let [peer (rand-nth @cluster)]
    ((:shutdown-fn peer))))

(def linear-cluster-test
  (sim/create-test sim-conn
                   linear-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration (u/hours->msec 1)}))

(def linear-cluster-sim
  (sim/create-sim sim-conn
                  linear-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

(sim/create-fixed-clock sim-conn linear-cluster-sim {:clock/multiplier 1})

(sim/create-action-log sim-conn linear-cluster-sim)

(def pruns
  (->> #(sim/run-sim-process sim-uri (:db/id linear-cluster-sim))
       (repeatedly (:sim/processCount linear-cluster-sim))
       (into [])))

(def results (hq-util/read! hq-config out-queue (inc n-messages) echo))

(doseq [prun pruns] (future-cancel (:runner prun)))

(onyx.api/shutdown conn)

(doseq [peer @cluster]
  ((:shutdown-fn peer)))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

