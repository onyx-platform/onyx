(ns onyx.coordinator.sine-cluster-sim
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

(defn create-multi-births [executor t k]
  (mapcat
   (constantly
    [[{:db/id (d/tempid :test)
       :agent/_actions (u/e executor)
       :action/atTime t
       :action/type :action.type/register-sine-peer}]])
   (range k)))

(defn create-multi-deaths [executor t k]
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
    (mapcat (fn [[t delta]] (if (>= delta 0)
                             (create-multi-births executor t delta)
                             (create-multi-deaths executor t (Math/abs delta))))
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

(sim-utils/load-schema sim-conn "simulant/coordinator-sim.edn")

(def id (str (java.util.UUID/randomUUID)))

(def system (onyx-coordinator
             {:hornetq-addr "localhost:5445"
              :zk-addr "127.0.0.1:2181"
              :onyx-id id
              :revoke-delay 2000}))

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

(def n-jobs 100)

(def tasks-per-job 3)

(def job-chs (map (fn [_] (chan 1)) (range n-jobs)))

(tap (:offer-mult coordinator) offer-spy)

(doseq [n (range n-jobs)]
  (>!! (:planning-ch-head coordinator)
       [{:catalog catalog :workflow workflow} (nth job-chs n)]))

(doseq [_ (range n-jobs)]
  (<!! offer-spy))

(def sine-model-id (d/tempid :model))

(def sine-cluster-model-data
  [{:db/id sine-model-id
    :model/type :model.type/sine-cluster
    :model/n-peers 100
    :model/peek-peers 20
    :model/peer-rate 50
    :model/sine-length 40000
    :model/sine-start 2000
    :model/sine-reps 8
    :model/mean-ack-time 250
    :model/mean-completion-time 500}])

(def sine-cluster-model
  (-> @(d/transact sim-conn sine-cluster-model-data)
      (u/tx-ent sine-model-id)))

(defmethod sim/perform-action :action.type/register-sine-peer
  [action process]
  (let [peer (extensions/create (:sync components) :peer)]
    (swap! cluster assoc peer
           (sim-utils/create-peer
            sine-cluster-model
            components peer))))

(defmethod sim/perform-action :action.type/unregister-sine-peer
  [action process]
  (try
    (let [cluster-val @cluster
          n (count cluster-val)
          victim (nth (keys cluster-val) (rand-int n))]
      (let [pulse (:pulse-node (extensions/read-place (:sync components) (:node victim)))]
        (extensions/delete (:sync components) pulse)
        (future-cancel (get cluster-val victim))
        (swap! cluster dissoc victim)))
    (catch Exception e
      (.printStackTrace e))))

(sim-utils/create-peers! sine-cluster-model components cluster)

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

(sim/create-fixed-clock sim-conn sine-cluster-sim {:clock/multiplier 1})

(sim/create-action-log sim-conn sine-cluster-sim)

(def pruns
  (->> #(sim/run-sim-process sim-uri (:db/id sine-cluster-sim))
       (repeatedly (:sim/processCount sine-cluster-sim))
       (into [])))

(doseq [job-ch job-chs]
  @(onyx.api/await-job-completion* (:sync components) (str (<!! job-ch))))

(doseq [prun pruns] (future-cancel (:runner prun)))

(facts "All tasks of all jobs are completed"
       (sim-utils/task-completeness (:sync coordinator)))

(facts "Peer states only make legal transitions"
       (sim-utils/peer-state-transition-correctness (:sync coordinator)))

(facts "Sequential tasks are only executed by one peer at a time"
       (sim-utils/sequential-safety (:sync coordinator)))

(component/stop components)

