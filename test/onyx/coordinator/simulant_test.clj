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
            [onyx.coordinator.sim-test-utils :as su]
            [incanter.core :refer [view]]
            [incanter.charts :refer [line-chart]]))

(def cluster (atom {}))

(defn create-peer [model components peer]
  (future
    (let [coordinator (:coordinator components)
          sync (:sync components)
          payload (extensions/create sync :payload)
          sync-spy (chan 1)
          status-spy (chan 1)]
      (extensions/write-place sync peer payload)
      (extensions/on-change sync payload #(>!! sync-spy %))
         
      (>!! (:born-peer-ch-head coordinator) peer)

      (loop [payload-node payload]
        (<!! sync-spy)
        (<!! (timeout (gen/geometric (/ 1 (:model/mean-ack-time model)))))

        (let [nodes (:nodes (extensions/read-place sync payload-node))]
          (extensions/on-change sync (:status nodes) #(>!! status-spy %))
          (extensions/touch-place sync (:ack nodes))
          (<!! status-spy)
          (<!! (timeout (gen/geometric (/ 1 (:model/mean-completion-time model)))))

          (let [next-payload (extensions/create sync :payload)]
            (extensions/write-place sync peer next-payload)
            (extensions/on-change sync next-payload #(>!! sync-spy %))
            (extensions/touch-place sync (:completion nodes))

            (recur next-payload)))))))

(defn create-peers! [model components]
  (doseq [_ (range (:model/n-peers model))]
    (let [peer (extensions/create (:sync components) :peer)]
      (swap! cluster assoc peer (create-peer model components peer)))))

#_(defn create-fixed-cluster-test [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/fixed-cluster
                           :model/_tests (u/e model))])
      (u/tx-ent (:db/id test))))

(defn create-linear-cluster-test [conn model test]
  (u/require-keys test :db/id :test/duration)
  (-> @(d/transact conn [(assoc test
                           :test/type :test.type/linear-cluster
                           :model/_tests (u/e model))])
      (u/tx-ent (:db/id test))))

#_(defmethod sim/create-test :model.type/fixed-cluster
  [conn model test]
  (let [test (create-fixed-cluster-test conn model test)]
    (d/entity (d/db conn) (u/e test))))

(defn create-executor [conn test]
  (let [tid (d/tempid :test)
        result @(d/transact conn
                            [{:db/id tid
                              :agent/type :agent.type/executor
                              :test/_agents (u/e test)}])]
    (d/resolve-tempid (d/db conn) (:tempids result) tid)))

(defn create-birth [executor t]
  (prn "Birth: " t)
  [[{:db/id (d/tempid :test)
     :agent/_actions (u/e executor)
     :action/atTime t
     :action/type :action.type/register-peer}]])

(defn create-death [executor t]
  (prn "Death: " t)
  [[{:db/id (d/tempid :test)
     :agent/_actions (u/e executor)
     :action/atTime t
     :action/type :action.type/unregister-peer}]])

(defn generate-linear-scaling-data [test executor]
  (let [model (-> test :model/_tests first)
        limit (:test/duration test)
        peers (:model/peek-peers model)
        births (mapcat (partial create-birth executor)
                       (range 0 (* peers 1000) 1000))
        deaths (mapcat (partial create-death executor)
                       (range (- limit (* peers 1000)) limit 1000))]
    (concat births deaths)))

(defmethod sim/create-test :model.type/linear-cluster
  [conn model test]
  (let [test (create-linear-cluster-test conn model test)
        executor (create-executor conn test)]
    (u/transact-batch conn (generate-linear-scaling-data test executor) 1000)
    (d/entity (d/db conn) (u/e test))))

#_(defmethod sim/create-sim :test.type/fixed-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(defmethod sim/create-sim :test.type/linear-cluster
  [sim-conn test sim]
  (-> @(d/transact sim-conn (sim/construct-basic-sim test sim))
      (u/tx-ent (:db/id sim))))

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (su/reset-conn sim-uri))

(su/load-schema sim-conn "simulant/schema.edn")

(su/load-schema sim-conn "simulant/coordinator-sim.edn")

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq :revoke-delay 500000}))

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

(def n-jobs 3)

(def n-peers 10)

(def tasks-per-job 3)

(tap (:offer-mult coordinator) offer-spy)

(doseq [_ (range n-jobs)]
  (>!! (:planning-ch-head coordinator) {:catalog catalog :workflow workflow}))

(doseq [_ (range n-jobs)]
  (<!! offer-spy))

#_(def fixed-model-id (d/tempid :model))

(def linear-model-id (d/tempid :model))

#_(def fixed-cluster-model-data
  [{:db/id fixed-model-id
    :model/type :model.type/fixed-cluster
    :model/n-peers n-peers
    :model/mean-ack-time 5000
    :model/mean-completion-time 15000}])

(def linear-cluster-model-data
  [{:db/id linear-model-id
    :model/type :model.type/linear-cluster
    :model/n-peers 2
    :model/peek-peers 5
    :model/mean-ack-time 250
    :model/mean-completion-time 500}])

#_(def fixed-cluster-model
  (-> @(d/transact sim-conn fixed-cluster-model-data)
      (u/tx-ent fixed-model-id)))

(def linear-cluster-model
  (-> @(d/transact sim-conn linear-cluster-model-data)
      (u/tx-ent linear-model-id)))

(defmethod sim/perform-action :action.type/register-peer
  [action process]
  (let [peer (extensions/create (:sync components) :peer)]
      (swap! cluster assoc peer (create-peer linear-cluster-model components peer))))

(defmethod sim/perform-action :action.type/unregister-peer
  [action process]
  (let [cluster-val @cluster
        n (count cluster-val)
        victim (nth (keys cluster-val) (rand-int n))]
    (future-cancel (get cluster-val victim))
    (extensions/delete (:sync components) victim)
    (swap! cluster dissoc victim)))

#_(create-peers! fixed-cluster-model components)

(create-peers! linear-cluster-model components)

#_(def fixed-cluster-test
  (sim/create-test sim-conn
                   fixed-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration 15000}))

(def linear-cluster-test
  (sim/create-test sim-conn
                   linear-cluster-model
                   {:db/id (d/tempid :test)
                    :test/duration 20000}))

#_(def fixed-cluster-sim
  (sim/create-sim sim-conn
                  fixed-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

(def linear-cluster-sim
  (sim/create-sim sim-conn
                  linear-cluster-test
                  {:db/id (d/tempid :sim)
                   :sim/systemURI (str "datomic:mem://" (d/squuid))
                   :sim/processCount 1}))

#_(sim/create-fixed-clock sim-conn fixed-cluster-sim {:clock/multiplier 1})

(sim/create-fixed-clock sim-conn linear-cluster-sim {:clock/multiplier 1})

#_(sim/create-action-log sim-conn fixed-cluster-sim)

(sim/create-action-log sim-conn linear-cluster-sim)

(time (mapv (fn [prun] @(:runner prun))
            (->> #(sim/run-sim-process sim-uri (:db/id linear-cluster-sim))
                 (repeatedly (:sim/processCount linear-cluster-sim))
                 (into []))))

(comment "Run the fixed sim"
         (time (mapv (fn [prun] @(:runner prun))
                     (->> #(sim/run-sim-process sim-uri (:db/id fixed-cluster-sim))
                          (repeatedly (:sim/processCount fixed-cluster-sim))
                          (into [])))))

(testing "All tasks complete"
  (loop []
    (let [db (:db-after (.take tx-queue))
          query '[:find (count ?task) :where [?task :task/complete? true]]
          result (ffirst (d/q query db))]
      (prn result)
      (when-not (= result (* n-jobs tasks-per-job))
        (recur)))))

(def sim-db (d/db sim-conn))

(def result-db (d/db (:conn log)))

(deftest test-small-cluster-few-jobs
  (testing "No tasks are left incomplete"
    (su/task-completeness result-db))

  (testing "No sequential task ever had more than 1 peer"
    (su/task-safety result-db))

  (testing "No peers got 0 tasks"
  ;;  (su/peer-liveness result-db n-peers)
    )

  (testing "All peers got a roughly even number of tasks assigned"
;;    (su/peer-fairness result-db n-peers n-jobs tasks-per-job)
    ))

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

(run-tests 'onyx.coordinator.simulant-test)

