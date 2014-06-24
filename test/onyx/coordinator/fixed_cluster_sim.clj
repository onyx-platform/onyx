(ns onyx.coordinator.fixed-cluster-sim
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan <!! >!! tap timeout]]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [datomic.api :as d]
            [onyx.system :refer [onyx-coordinator]]
            [onyx.extensions :as extensions]
            [onyx.coordinator.sim-test-utils :as sim-utils]
            [onyx.api]))

(def cluster (atom {}))

(defn create-fixed-cluster-test [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/fixed-cluster
                           :model/_tests (u/e model))])
      (u/tx-ent (:db/id test))))

(defmethod sim/create-test :model.type/fixed-cluster
  [conn model test]
  (let [test (create-fixed-cluster-test conn model test)]
    (d/entity (d/db conn) (u/e test))))

(defmethod sim/create-sim :test.type/fixed-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (sim-utils/reset-conn sim-uri))

(sim-utils/load-schema sim-conn "simulant/schema.edn")

(sim-utils/load-schema sim-conn "simulant/coordinator-sim.edn")

(def id (str (java.util.UUID/randomUUID)))

(def system (onyx-coordinator
             {:hornetq-addr "localhost:5445"
              :zk-addr "127.0.0.1:2181"
              :onyx-id id
              :revoke-delay 500000}))

(def components (component/start system))

(def coordinator (:coordinator components))

(def offer-spy (chan 10000))

(def catalog
  [{:onyx/name :in
    :onyx/type :input
    :onyx/consumption :sequential
    :onyx/medium :hornetq
    :hornetq/queue-name "in-queue"}
   {:onyx/name :inc
    :onyx/type :transformer
    :onyx/consumption :sequential}
   {:onyx/name :out
    :onyx/type :output
    :onyx/consumption :sequential
    :onyx/medium :hornetq
    :hornetq/queue-name "out-queue"}])

(def workflow {:in {:inc :out}})

(def n-jobs 10)

(def n-peers 5)

(def tasks-per-job 3)

(def job-chs (map (fn [_] (chan 1)) (range n-jobs)))

(tap (:offer-mult coordinator) offer-spy)

(doseq [n (range n-jobs)]
  (>!! (:planning-ch-head coordinator)
       [{:catalog catalog :workflow workflow} (nth job-chs n)]))

(doseq [_ (range n-jobs)]
  (<!! offer-spy))

(def fixed-model-id (d/tempid :model))

(def fixed-cluster-model-data
  [{:db/id fixed-model-id
    :model/type :model.type/fixed-cluster
    :model/n-peers n-peers
    :model/mean-ack-time 250
    :model/mean-completion-time 500}])

(def fixed-cluster-model
  (-> @(d/transact sim-conn fixed-cluster-model-data)
      (u/tx-ent fixed-model-id)))

(sim-utils/create-peers! fixed-cluster-model components cluster)

(def fixed-cluster-test
  (sim/create-test sim-conn
                   fixed-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration 15000}))

(def fixed-cluster-sim
  (sim/create-sim sim-conn
                  fixed-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

(sim/create-fixed-clock sim-conn fixed-cluster-sim {:clock/multiplier 1})

(sim/create-action-log sim-conn fixed-cluster-sim)

(future
  (mapv (fn [prun] @(:runner prun))
        (->> #(sim/run-sim-process sim-uri (:db/id fixed-cluster-sim))
             (repeatedly (:sim/processCount fixed-cluster-sim))
             (into []))))

(doseq [job-ch job-chs]
  @(onyx.api/await-job-completion* (:sync components) (str (<!! job-ch))))

(def sim-db (d/db sim-conn))

(facts "All tasks of all jobs are completed"
       (sim-utils/task-completeness (:sync coordinator)))

(facts "All peers got at least one task"
       (sim-utils/peer-liveness (:sync coordinator)))

(facts "Tasks are fairly distributed amongst peers"
       (sim-utils/peer-fairness (:sync coordinator) n-peers n-jobs tasks-per-job))

(facts "Peer states only make legal transitions"
       (sim-utils/peer-state-transition-correctness (:sync coordinator)))

(facts "Sequential tasks are only executed by one peer at a time"
       (sim-utils/sequential-safety (:sync coordinator)))

(component/stop components)

