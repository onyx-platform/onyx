(ns onyx.coordinator.sine-cluster-sim
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan <!! >!! tap timeout]]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [datomic.api :as d]
            [onyx.system :as s]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.coordinator.sim-test-utils :as sim-utils]
            [incanter.core :refer [view]]
            [incanter.charts :refer [line-chart]]))

(def cluster (atom {}))

(defn create-birth [executor t k]
  (mapcat
   (constantly
    [[{:db/id (d/tempid :test)
       :agent/_actions (u/e executor)
       :action/atTime t
       :action/type :action.type/register-sine-peer}]])
   (range k)))

(defn create-death [executor t k]
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
                             (create-birth executor t delta)
                             (create-death executor t (Math/abs delta))))
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
        executor (create-executor conn test)]
    (u/transact-batch conn (generate-sine-scaling-data test executor) 1000)
    (d/entity (d/db conn) (u/e test))))

(defmethod sim/create-sim :test.type/sine-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (sim-utils/reset-conn sim-uri))

(sim-utils/load-schema sim-conn "simulant/schema.edn")

(sim-utils/load-schema sim-conn "simulant/coordinator-sim.edn")

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq :revoke-delay 2000}))

(def components (alter-var-root #'system component/start))

(def coordinator (:coordinator components))

(def log (:log components))

(def tx-queue (d/tx-report-queue (:conn log)))

(def offer-spy (chan 10000))

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

(def n-jobs 20)

(def tasks-per-job 3)

(tap (:offer-mult coordinator) offer-spy)

(doseq [_ (range n-jobs)]
  (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow}))

(doseq [_ (range n-jobs)]
  (<!! offer-spy))

(def sine-model-id (d/tempid :model))

(def sine-cluster-model-data
  [{:db/id sine-model-id
    :model/type :model.type/sine-cluster
    :model/n-peers 25
    :model/peek-peers 20
    :model/peer-rate 50
    :model/sine-length 20000
    :model/sine-start 5000
    :model/sine-reps 8
    :model/mean-ack-time 250
    :model/mean-completion-time 500}])

(def sine-cluster-model
  (-> @(d/transact sim-conn sine-cluster-model-data)
      (u/tx-ent sine-model-id)))

(defmethod sim/perform-action :action.type/register-sine-peer
  [action process]
  (prn "up")
  (let [peer (extensions/create (:sync components) :peer)]
    (swap! cluster assoc peer
           (sim-utils/create-peer
            sine-cluster-model
            components peer))))

(defmethod sim/perform-action :action.type/unregister-sine-peer
  [action process]
  (prn "down")
  (let [cluster-val @cluster
        n (count cluster-val)
        victim (nth (keys cluster-val) (rand-int n))]
    (let [pulse (:pulse (extensions/read-place (:sync components) victim))]
      (extensions/delete (:sync components) pulse)
      (future-cancel (get cluster-val victim))
      (swap! cluster dissoc victim))))

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

(testing "All tasks complete"
  (loop []
    (let [db (:db-after (.take tx-queue))
          query '[:find (count ?task) :where [?task :task/complete? true]]
          result (ffirst (d/q query db))]
      (prn result)
      (prn "Peers ->>" (d/q '[:find (count ?p) :where [?p :peer/status]] db))
      (when-not (= result (* n-jobs tasks-per-job))
        (recur)))))

(doseq [prun pruns] (future-cancel (:runner prun)))

(def sim-db (d/db sim-conn))

(def result-db (d/db (:conn log)))

(deftest test-small-cluster-few-jobs
  (testing "No tasks are left incomplete"
    (sim-utils/task-completeness result-db))

  (testing "No sequential task ever had more than 1 peer"
    (sim-utils/task-safety result-db)))

(def insts
  (->> (-> '[:find ?inst :where
             [_ :peer/status _ ?tx]
             [?tx :db/txInstant ?inst]]
           (d/q (d/history result-db)))
       (map first)
       (sort)))

(def dt-and-peers
  (map (fn [tx]
         (let [db (d/as-of result-db tx)]
           (->> (d/q '[:find (count ?p) :where [?p :peer/status]] db)
                (map first)
                (concat [tx]))))
       insts))

(view (line-chart
       (map first dt-and-peers)
       (map second dt-and-peers)
       :x-label "Time"
       :y-label "Peers"))

(alter-var-root #'system component/stop)

