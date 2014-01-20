(ns onyx.coordinator.coordinator-simulant-tests
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! tap]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :refer [tx-ent e hours->msec getx] :as u]
            [datomic.api :as d]
            [onyx.system :as s]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]))

(defn reset-conn
  "Reset connection to a scratch database. Use memory database if no
   URL passed in."
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

(defn generate-execution [test peer peers at-time]
  (let [model (-> test :model/_tests u/solo)]
    [[{:db/id (d/tempid :test)
       :agent/_actions (e peer)
       :action/atTime at-time
       :action/type :action.type/execute-task}]]))

(defn generate-peer-executions [test peer peers]
  (let [model (-> test :model/_tests u/solo)
        limit (:test/duration test)]
    (->> (range 0 limit 5000)
         (take-while (fn [t] (< t limit)))
         (mapcat #(generate-execution test peer peers %)))))

(defn generate-all-executions [test peers]
  (mapcat
   (fn [peer] (generate-peer-executions test peer peers))
   peers))

(defmethod sim/create-test :model.type/stubbed-peer
  [conn model test]
  (let [test (create-test conn model test)
        peers (create-peers conn test)]
    (u/transact-batch conn (generate-all-executions test peers) 1000)
    (d/entity (d/db conn) (e test))))

(defmethod sim/create-sim :test.type/stubbed-peer
  [sim-conn test sim]
  (let [model (-> test :model/_tests u/solo)]
    
    (doseq [peer (:test/agents test)]
      (let [peer-node (extensions/create sync-storage :peer)
            payload-node (extensions/create sync-storage :payload)
            sync-watch (chan 1)]
        (extensions/write-place sync-storage peer-node payload-node)
        (extensions/on-change sync-storage payload-node #(>!! sync-watch %))
        (>!! (:born-peer-ch-head coordinator) peer-node)))
    
    (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
        (tx-ent (:db/id sim)))))

(defmethod sim/perform-action :action.type/execute-task
  [action process])

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

(defn assoc-codebase-tx [entities]
  (let [codebase (u/gen-codebase)
        cid (:db/id codebase)]
    (cons
     codebase
     (mapv #(assoc {:db/id (:db/id %)} :source/codebase cid) entities))))

@(d/transact sim-conn (assoc-codebase-tx [coordinator-test coordinator-sim]))

(def action-log (sim/create-action-log sim-conn coordinator-sim))

(def sim-clock (sim/create-fixed-clock sim-conn coordinator-sim {:clock/multiplier 960}))

(def pruns
  (->> #(sim/run-sim-process sim-uri (:db/id coordinator-sim))
       (repeatedly (:sim/processCount coordinator-sim))
       (into [])))

(time
 (mapv (fn [prun] @(:runner prun)) pruns))

(alter-var-root #'system component/stop)

