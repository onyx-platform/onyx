(ns onyx.log.simulant.sine-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [onyx.peer.simulant.sim-test-utils :as sim-utils]
            [datomic.api :as d]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def cluster (atom []))

(defn create-peer [executor t k]
  (mapcat
   (constantly
    [[{:db/id (d/tempid :test)
       :agent/_actions (u/e executor)
       :action/atTime t
       :action/type :action.type/register-sine-peer}]])
   (range k)))

(defn kill-peer [executor t k]
  (mapcat
   (constantly
    [[{:db/id (d/tempid :test)
       :agent/_actions (u/e executor)
       :action/atTime t
       :action/type :action.type/unregister-sine-peer}]])
   (range k)))

(defn generate-sine-scaling-data [test executor]
  (let [model (-> test :model/_tests first)
        limit (:test/duration test)
        length (:model/sine-length model)
        rate (:model/peer-rate model)
        reps (:model/sine-reps model)
        height (:model/peek-peers model)
        start (:model/sine-start model)
        end (+ start length)
        unit (/ (* reps Math/PI) length)
        wave (map (fn [x] (int (* height (Math/sin (* unit x)))))
                  (range 0 (+ end rate) rate))
        deltas (map (fn [[a b]] (- b a)) (partition 2 1 wave))]
    (mapcat (fn [[t delta]]
              (if (>= delta 0)
                (create-peer executor t delta)
                (kill-peer executor t (Math/abs delta))))
            (map vector (range start end rate) deltas))))

(defn create-sine-cluster-test [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/sine-cluster
                           :model/_tests (u/e model))])
      (u/tx-ent (:db/id test))))

(defn create-executor [conn test]
  (let [tid (d/tempid :test)
        result @(d/transact conn
                            [{:db/id tid
                              :agent/type :agent.type/executor
                              :test/_agents (u/e test)}])]
    (d/resolve-tempid (d/db conn) (:tempids result) tid)))

(defmethod sim/create-test :model.type/sine-cluster
  [conn model test]
  (let [test (create-sine-cluster-test conn model test)
        executor (create-executor conn test)
        actions (generate-sine-scaling-data test executor)]
    (u/transact-batch conn actions 1000)
    (d/entity (d/db conn) (u/e test))))

(defmethod sim/create-sim :test.type/sine-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (sim-utils/reset-conn sim-uri))

(sim-utils/load-schema sim-conn "simulant/schema.edn")

(sim-utils/load-schema sim-conn "simulant/peer-sim.edn")

(def sine-model-id (d/tempid :model))

(def sine-cluster-model-data
  [{:db/id sine-model-id
    :model/type :model.type/sine-cluster
    :model/peek-peers 15
    :model/peer-rate 500
    :model/sine-length (u/hours->msec 1)
    :model/sine-start 0
    :model/sine-reps 8}])

(def sine-cluster-model
  (-> @(d/transact sim-conn sine-cluster-model-data)
      (u/tx-ent sine-model-id)))

(defmethod sim/perform-action :action.type/register-sine-peer
  [action process]
  (when (< (count @cluster) 30)
    (let [peer (first (onyx.api/start-peers 1 peer-config))]
      (swap! cluster conj peer))))

(defmethod sim/perform-action :action.type/unregister-sine-peer
  [action process]
  (swap! cluster
         (fn [c]
           (when (last c)
             (onyx.api/shutdown-peer (last c)))
           (vec (butlast c)))))

(def sine-cluster-test
  (sim/create-test sim-conn
                   sine-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration (u/hours->msec 1)}))

(def sine-cluster-sim
  (sim/create-sim sim-conn
                  sine-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

(sim/create-fixed-clock sim-conn sine-cluster-sim {:clock/multiplier 150})

(sim/create-action-log sim-conn sine-cluster-sim)

;; Seed it with 20 peers since sine waves goes negative.
(doseq [peer (onyx.api/start-peers 20 peer-config)]
  (swap! cluster conj peer))

(def pruns
  (->> #(sim/run-sim-process sim-uri (:db/id sine-cluster-sim))
       (repeatedly (:sim/processCount sine-cluster-sim))
       (into [])))

(doseq [prun pruns] @(:runner prun))

;; We should finish with 15 peers. Take it to a global maximum
;; to have a reliable seek point in the log for verification.
(doseq [peer (onyx.api/start-peers 30 peer-config)]
  (swap! cluster conj peer))

(def ch (chan 5))

(def replica
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (< (count (:pairs new-replica)) 45)
        (recur new-replica)
        new-replica))))

(fact (count (:peers replica)) => 45)

(doseq [peer @cluster]
  (onyx.api/shutdown-peer peer))

(onyx.api/shutdown-env env)

