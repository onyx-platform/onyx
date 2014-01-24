(ns onyx.coordinator.simulant-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! tap timeout]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [datomic.api :as d]
            [onyx.system :as s]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.coordinator.sim-test-utils :refer [with-system reset-conn load-schema]]))

(def cluster (atom []))

(defn create-peer [model components]
  (future
    (let [coordinator (:coordinator components)
          sync (:sync components)
          peer (extensions/create sync :peer)
          payload (extensions/create sync :payload)
          sync-spy (chan 1)
          status-spy (chan 1)]
      (extensions/write-place sync peer payload)
      (extensions/on-change sync payload #(>!! sync-spy %))
         
      (>!! (:born-peer-ch-head coordinator) peer)

      (loop [payload-node payload]
        (<!! sync-spy)

        (let [nodes (:nodes (extensions/read-place sync payload-node))]
          (extensions/on-change sync (:status nodes) #(>!! status-spy %))
          (extensions/touch-place sync (:ack nodes))
          (<!! status-spy)

          (let [next-payload (extensions/create sync :payload)]
            (extensions/write-place sync peer next-payload)
            (extensions/on-change sync next-payload #(>!! sync-spy %))
            (extensions/touch-place sync (:completion nodes))

            (recur next-payload)))))))

(defn create-peers! [model components]
  (doseq [_ (range (:model/n-peers model))]
    (swap! cluster conj (create-peer model components))))

(defn create-test [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/fixed-cluster
                           :model/_tests (u/e model))])
      (u/tx-ent (:db/id test))))

(defmethod sim/create-test :model.type/fixed-cluster
  [conn model test]
  (let [test (create-test conn model test)]
    (d/entity (d/db conn) (u/e test))))

(defmethod sim/create-sim :test.type/fixed-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(defmethod sim/perform-action :action.type/register-peer
  [action process])

(defmethod sim/perform-action :action.type/unregister-peer
  [action process])

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (reset-conn sim-uri))

(load-schema sim-conn "simulant/schema.edn")

(load-schema sim-conn "simulant/coordinator-sim.edn")

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq :eviction-delay 500000}))

(def components (alter-var-root #'system component/start))

(def model-id (d/tempid :model))

(def fixed-cluster-model-data
  [{:db/id model-id
    :model/type :model.type/fixed-cluster
    :model/n-peers 10
    :model/mean-ack-time 5000
    :model/mean-completion-time 15000}])

(def fixed-cluster-model
  (-> @(d/transact sim-conn fixed-cluster-model-data)
      (u/tx-ent model-id)))

(create-peers! fixed-cluster-model components)

(def fixed-cluster-test
  (sim/create-test sim-conn
                   fixed-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration (u/hours->msec 1)}))

(def fixed-cluster-sim
  (sim/create-sim sim-conn
                  fixed-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

(sim/create-fixed-clock sim-conn fixed-cluster-sim {:clock/multiplier 960})

(sim/create-action-log sim-conn fixed-cluster-sim)

(time (mapv (fn [prun] @(:runner prun))
                (->> #(sim/run-sim-process sim-uri (:db/id fixed-cluster-sim))
                     (repeatedly (:sim/processCount fixed-cluster-sim))
                     (into []))))

(def sim-db (d/db sim-conn))

(def result-db (d/db (:conn (:log components))))


(alter-var-root #'system component/stop)

