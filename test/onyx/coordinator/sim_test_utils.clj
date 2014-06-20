(ns onyx.coordinator.sim-test-utils
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan <!! >!! timeout]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [taoensso.timbre :refer [info]]
            [onyx.coordinator.impl :as impl]
            [onyx.extensions :as extensions]
            [onyx.system :refer [onyx-coordinator]]))

(defn with-system [f & opts]
  (let [id (str (java.util.UUID/randomUUID))
        defaults {:hornetq-addr "localhost:5445"
                  :zk-addr "127.0.0.1:2181"
                  :onyx-id id
                  :revoke-delay 4000}]
    (let [system (onyx-coordinator (apply merge defaults opts))
          live (component/start system)
          coordinator (:coordinator live)
          sync (:sync live)]
      (try
        (f coordinator sync)
        (finally
         (component/stop live))))))

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

(defn entity-txs [db ent]
  (map #(nth % 3)
       (d/q
        '[:find ?e ?a ?v ?tx ?added
          :in $ ?e
          :where
          [?e ?a ?v ?tx ?added]]
        (datomic.api/history db)
        ent)))

(defn sequential-task-ids [db]
  (->> (d/history db)
       (d/q '[:find ?task :where [?task :task/consumption :sequential]])
       (map first)
       (into #{})))

(defn concurrent-task-ids [db]
  (->> (d/history db)
       (d/q '[:find ?task :where [?task :task/consumption :concurrent]])
       (map first)
       (into #{})))

(defn task-completeness [sync]
  (let [job-nodes (extensions/bucket sync :job)
        task-paths (map #(extensions/resolve-node sync :task %) job-nodes)]
    (doseq [task-path task-paths]
      (doseq [task-node (extensions/children sync task-path)]
        (when-not (impl/completed-task? task-node)
          (fact (impl/task-complete? sync task-node) => true))))))

(defn sequential-safety [result-db]
  (let [task-ids (sequential-task-ids result-db)
        task-txs (mapcat (partial entity-txs result-db) task-ids)
        result (mapcat
                #(let [db (d/as-of result-db %)
                       query '[:find ?task (count ?peer) :where
                               [?task :task/consumption :sequential]
                               [?peer :peer/task ?task]]]
                   (map second (d/q query db)))
                task-txs)]
    
    (fact "No sequential tasks ever had more than one peer at a time" 
          (every? (partial = 1) result) => true)))

(defn peer-liveness [sync]
  (doseq [state-path (extensions/bucket sync :peer-state)]
    (let [states (extensions/children sync state-path)
          state-data (map (partial extensions/read-place sync) states)
          active-states (filter #(= (:state %) :active) state-data)]
      (fact (count active-states) =not=> zero?))))

(defn peer-fairness [result-db n-peers n-jobs tasks-per-job]
  (let [query '[:find ?peer (count ?task) :where
                [?peer :peer/task ?task]]
        result (map second (d/q query (d/history result-db)))
        mean (/ (* n-jobs tasks-per-job) n-peers)
        confidence 0.75]
    
    (fact "All peers got within 75% of the average number of tasks"
          (every?
           #(and (<= (- mean (* mean confidence)) %)
                 (>= (+ mean (* mean confidence)) %))
           result)) => true))

(defn create-peer [model components peer]
  (future
    (try
      (let [coordinator (:coordinator components)
            sync (:sync components)
            payload (extensions/create sync :payload)
            pulse (extensions/create sync :pulse)
            shutdown (extensions/create sync :shutdown)
            sync-spy (chan 1)
            status-spy (chan 1)]
        (extensions/write-place sync (:node peer)
                                {:id (:uuid peer)
                                 :peer-node (:node peer)
                                 :pulse-node (:node pulse)
                                 :shutdown-node (:node shutdown)
                                 :payload-node (:node payload)})
        (extensions/on-change sync (:node payload) #(>!! sync-spy %))
        
        (>!! (:born-peer-ch-head coordinator) (:node peer))

        (loop [p payload]
          (<!! sync-spy)
          (<!! (timeout (gen/geometric (/ 1 (:model/mean-ack-time model)))))

          (let [nodes (:nodes (extensions/read-place sync (:node p)))]
            (extensions/on-change sync (:node/status nodes) #(>!! status-spy %))
            (extensions/touch-place sync (:node/ack nodes))
            (<!! status-spy)
            (<!! (timeout (gen/geometric (/ 1 (:model/mean-completion-time model)))))

            (let [next-payload (extensions/create sync :payload)]
              (extensions/write-place sync (:node peer) {:id (:uuid peer)
                                                         :peer-node (:node peer)
                                                         :pulse-node (:node pulse)
                                                         :shutdown-node (:node shutdown)
                                                         :payload-node (:node next-payload)})
              (extensions/on-change sync (:node next-payload) #(>!! sync-spy %))
              (extensions/touch-place sync (:node/completion nodes))

              (recur next-payload)))))
      (catch Exception e (prn "Peer: " e)))))

(defn create-peers! [model components cluster]
  (doseq [_ (range (:model/n-peers model))]
    (let [peer (extensions/create (:sync components) :peer)]
      (swap! cluster assoc peer (create-peer model components peer)))))

