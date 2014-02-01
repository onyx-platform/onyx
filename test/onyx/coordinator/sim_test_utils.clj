(ns onyx.coordinator.sim-test-utils
  (:require [midje.sweet :refer :all]
            [clojure.core.async :refer [chan <!! >!! timeout]]
            [clojure.data.generators :as gen]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [incanter.core :refer [view]]
            [incanter.charts :refer [line-chart]]
            [onyx.coordinator.extensions :as extensions]
            [onyx.system :as s]))

(defn with-system [f & opts]
  (def system (s/onyx-system (apply merge {:sync :zookeeper :queue :hornetq :revoke-delay 4000} opts)))
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

(defn task-completeness [result-db]
  (let [query '[:find (count ?task) :where [?task :task/complete? false]]
        result (ffirst (d/q query result-db))]
    
    (fact "All tasks were completed"
          result => nil?)))

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

(defn peer-liveness [result-db n-peers]
  (let [query '[:find ?peer :where
                [?peer :peer/task]]
        result (map first (d/q query (d/history result-db)))]
    
    (fact "All peers got at least one task"
          (count result) => n-peers)))

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
        (extensions/write-place sync peer {:pulse pulse
                                           :shutdown shutdown
                                           :payload payload})
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
              (extensions/write-place sync peer {:pulse pulse :payload next-payload})
              (extensions/on-change sync next-payload #(>!! sync-spy %))
              (extensions/touch-place sync (:completion nodes))

              (recur next-payload)))))
      (catch Exception e (prn "Peer: " e)))))

(defn create-peers! [model components cluster]
  (doseq [_ (range (:model/n-peers model))]
    (let [peer (extensions/create (:sync components) :peer)]
      (swap! cluster assoc peer (create-peer model components peer)))))

(defn block-until-completion! [tx-queue total-tasks]
  (loop []
    (let [tx (.take tx-queue)
          db (:db-after tx)
          query '[:find (count ?task) :where [?task :task/complete? true]]
          result (ffirst (d/q query db))]
      (prn result)
      (when-not (= result total-tasks)
        (recur)))))

(defn view-peer-chart! [result-db]
  (let [insts (->> (-> '[:find ?inst :where
                         [_ :peer/status _ ?tx]
                         [?tx :db/txInstant ?inst]]
                       (d/q (d/history result-db)))
                   (map first)
                   (sort))
        dt-and-peers
        (map (fn [tx]
               (let [db (d/as-of result-db tx)]
                 (->> (d/q '[:find (count ?p) :where [?p :peer/status]] db)
                      (map first)
                      (concat [tx]))))
             insts)]
    (view (line-chart
           (map first dt-and-peers)
           (map second dt-and-peers)
           :x-label "Time"
           :y-label "Peers"))))

