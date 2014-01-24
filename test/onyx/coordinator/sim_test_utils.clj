(ns onyx.coordinator.sim-test-utils
  (:require [clojure.test :refer [is]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.system :as s]))

(defn with-system [f & opts]
  (def system (s/onyx-system (apply merge {:sync :zookeeper :queue :hornetq :eviction-delay 4000} opts)))
  (let [components (alter-var-root #'system component/start)
        coordinator (:coordinator components)
        sync (:sync components)
        log (:log components)]
    (try
      (f coordinator sync log)
      (finally
       (alter-var-root #'system component/stop)))))

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

(defn task-completeness [result-db]
  (let [query '[:find (count ?task) :where [?task :task/complete? false]]
        result (ffirst (d/q query result-db))]
    (is (nil? result))))

(defn task-safety [result-db]
  (let [query '[:find ?task (count ?peer) :where
                [?task :task/consumption :sequential]
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))]
    (is (every? (partial = 1) result))))

(defn peer-liveness [result-db n-peers]
  (let [query '[:find ?peer :where
                [?peer :peer/task]]
        result (map first (d/q query (d/history result-db)))]
    (is (= (count result) n-peers))))

(defn peer-fairness [result-db n-peers n-jobs tasks-per-job]
  (let [query '[:find ?peer (count ?task) :where
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))
        mean (/ (* n-jobs tasks-per-job) n-peers)
        confidence 0.5]
    (is (every?
         #(and (<= (- mean (* mean confidence)) %)
               (>= (+ mean (* mean confidence)) %))
         result))))

(defn concurrency-liveness [result-db n-jobs tasks-per-job]
  (let [query '[:find ?task (count ?peer) :where
                [?task :task/consumption :concurrent]
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))]
    (is (>= (count (filter (partial < 1) result))
            (/ (* n-jobs tasks-per-job) 0.25)))))

