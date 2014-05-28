(ns ^:no-doc onyx.coordinator.log.datomic
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre]
            [onyx.extensions :as extensions]
            [datomic.api :as d]))

(defn load-schema [file]
  (let [resource (clojure.java.io/resource file)]
    (read-string (slurp resource))))

(defn log-schema []
  (load-schema "datomic-schema.edn"))

(defn schema-set? [conn]
  (seq (d/q '[:find ?e :where [?e :db/ident :node/peer]] (d/db conn))))

(defn start-datomic! [uri schema]
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (when-not (schema-set? conn)
      @(d/transact conn schema))
    conn))

(defrecord Datomic [uri schema]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Datomic")
    (let [conn (start-datomic! uri schema)]
      (assoc component :conn conn)))

  (stop [component]
    (taoensso.timbre/info "Stopping Datomic")
    (d/delete-database uri)
    (d/shutdown false)
    component))

(defn datomic [uri schema]
  (map->Datomic {:uri uri :schema schema}))

(defn create-job-datom [catalog workflow tasks]
  {:db/id (d/tempid :onyx/log)
   :job/id (d/squuid)
   :job/catalog (pr-str catalog)
   :job/workflow (pr-str workflow)
   :job/task tasks})

(defn create-task-datom [task]
  {:db/id (d/tempid :onyx/log)
   :task/name (:name task)
   :task/phase (:phase task)
   :task/consumption (:consumption task)
   :task/complete? false
   :task/ingress-queues (:ingress-queues task)
   :task/egress-queues (or (vals (:egress-queues task)) [])})

(defn find-incomplete-tasks [db job-id]
  (let [query '[:find ?task :in $ ?job :where
                [?job :job/task ?task]
                [?task :task/complete? false]]]
    (d/q query db job-id)))

(defn find-incomplete-concurrent-tasks [db job-id]
  (let [query '[:find ?task :in $ ?job :where
                [?job :job/task ?task]
                [?task :task/consumption :concurrent]
                [?task :task/complete? false]]]
    (d/q query db job-id)))

(defn find-active-task-ids [db job-id tasks]
  (let [query '[:find ?task :in $ ?job ?task :where
                [?job :job/task ?task]
                [?peer :peer/task ?task]]]
    (into #{} (mapcat #(first (d/q query db job-id (:db/id %))) tasks))))

(defn peer-count [db task]
  (let [query '[:find (count ?peer) :in $ ?task :where
                [?peer :peer/task ?task]]]
    (or (ffirst (d/q query db (:db/id task))) 0)))

(defn sort-tasks-by-phase [db tasks]
  (sort-by :task/phase (map (fn [[t]] (d/entity db t)) tasks)))

(defn sort-tasks-by-peer-count [db tasks]
  (let [task-ents (map (fn [[t]] (d/entity db t)) tasks)
        task-peer-pair (map #(let [peers (peer-count db %)]
                               [% peers])
                            task-ents)]
    (map first (sort-by second task-peer-pair))))

(defn next-inactive-task [db job-id]
  (let [incomplete-tasks (find-incomplete-tasks db job-id)
        sorted-tasks (sort-tasks-by-phase db incomplete-tasks)
        active-tasks (find-active-task-ids db job-id sorted-tasks)]
    (filter (fn [t] (not (contains? active-tasks (:db/id t)))) sorted-tasks)))

(defn next-active-task [db job-id]
  (let [incomplete-tasks (find-incomplete-concurrent-tasks db job-id)]
    (sort-tasks-by-peer-count db incomplete-tasks)))

(defn all-active-jobs-ids [db]
  (->> db
       (d/q '[:find ?job ?tx :where
              [?job :job/task ?task ?tx]
              [?task :task/complete? false]])
       (sort-by second)
       (map first)))

(defn last-offered-job [db]
  (->> (d/history db)
       (d/q '[:find ?job ?tx :where
              [?job :job/task ?task ?tx]
              [?peer :peer/task ?task]
              [?peer :peer/status :acking]])
       (sort-by second)
       (last)
       (first)))

(defn job-candidate-seq [job-seq last-offered]
  (if (nil? last-offered)
    job-seq
    (->> (cycle job-seq)
         (drop (inc (.indexOf job-seq last-offered)))
         (take (count job-seq)))))

(defn to-task [db eid]
  (let [query '[:find ?id ?catalog ?workflow :in $ ?id :where
                [?job :job/task ?id]
                [?job :job/catalog ?catalog]
                [?job :job/workflow ?workflow]]
        result (first (d/q query db eid))
        [id catalog workflow] result]
    (assoc (into {} (d/entity db id))
      :db/id id
      :catalog catalog
      :workflow workflow)))

(defmethod extensions/mark-peer-born Datomic
  [log place]
  (let [tx [[:onyx.fn/add-peer (d/tempid :onyx/log) :idle place]]]
    @(d/transact (:conn log) tx)))

(defmethod extensions/mark-peer-dead Datomic
  [log place]
  (let [tx [[:onyx.fn/remove-peer place]]]
    @(d/transact (:conn log) tx)))

(defmethod extensions/plan-job Datomic
  [log catalog workflow tasks]
  (let [task-datoms (map create-task-datom tasks)
        job-datom (create-job-datom catalog workflow task-datoms)]
    @(d/transact (:conn log) [job-datom])
    (:job/id job-datom)))

(defmethod extensions/next-tasks Datomic
  [log]
  (let [db (d/db (:conn log))
        job-seq (job-candidate-seq (all-active-jobs-ids db)
                                   (last-offered-job db))
        inactive-candidates (mapcat (partial next-inactive-task db) job-seq)
        active-candidates (mapcat (partial next-active-task db) job-seq)
        ents (concat (filter identity inactive-candidates)
                     (filter identity active-candidates))
        eids (map :db/id ents)]
    (map (partial to-task db) eids)))

(defn select-nodes [ent]
  (select-keys ent [:node/peer :node/payload :node/ack :node/exhaust
                    :node/seal :node/status :node/completion]))

(defn node->task [db basis node]
  (let [query '[:find ?task :in $ ?basis ?node :where
                [?peer ?basis ?node]
                [?peer :peer/task ?task]]
        result (ffirst (d/q query db basis node))]
    (assoc (into {} (d/entity db result))
      :db/id result)))

(defn n-peers [db task-id]
  (let [query '[:find (count ?peer) :in $ ?task :where
                [?peer :peer/task ?task]
                [?peer :peer/status :active]]]
    (ffirst (d/q query db task-id))))

(defmethod extensions/nodes Datomic
  [log peer]
  (let [db (d/db (:conn log))
        query '[:find ?p :in $ ?peer :where [?p :node/peer ?peer]]
        result (ffirst (d/q query db peer))
        ent (into {} (d/entity db result))]
    (select-nodes ent)))

(defmethod extensions/node-basis Datomic
  [log basis node]
  (let [db (d/db (:conn log))
        query '[:find ?peer :in $ ?node-basis ?node-place :where
                [?peer ?node-basis ?node-place]]
        result (ffirst (d/q query db basis node))
        ent (into {} (d/entity db result))]
    (select-nodes ent)))

(defmethod extensions/node->task Datomic
  [log basis node]
  (let [db (d/db (:conn log))]
    (node->task db basis node)))

(defmethod extensions/idle-peers Datomic
  [log]
  (let [db (d/db (:conn log))
        peers (d/q '[:find ?place :where
                     [?peer :peer/status :idle]
                     [?peer :node/peer ?place]] db)]
    (map first peers)))

(defmethod extensions/seal-resource? Datomic
  [log exhaust-place]
  (let [db (d/db (:conn log))
        query '[:find ?task ?peer :in $ ?exhaust-node :where
                [?peer :peer/status :active]
                [?peer :peer/task ?task]
                [?peer :node/exhaust ?exhaust-node]
                [?task :task/complete? false]]
        result (d/q query db exhaust-place)
        task (ffirst result)
        peer (into {} (d/entity db (second (first result))))
        n (n-peers db task)]
    {:seal? (= n 1)
     :seal-node (:node/seal peer)}))

(defmethod extensions/mark-offered Datomic
  [log task peer nodes]
  (let [tx [[:onyx.fn/offer-task (:db/id task)
             (:task/consumption task) peer nodes]]]
    @(d/transact (:conn log) tx)))

(defmethod extensions/ack Datomic
  [log ack-place]
  (let [tx [[:onyx.fn/ack-task ack-place]]]
    @(d/transact (:conn log) tx)))

(defmethod extensions/revoke-offer Datomic
  [log ack-place]
  (let [tx [[:onyx.fn/revoke-offer ack-place]]]
    @(d/transact (:conn log) tx)))

(defmethod extensions/complete Datomic
  [log complete-place]
  (let [tx [[:onyx.fn/complete-task complete-place]]
        tx-result @(d/transact (:conn log) tx)
        db (:db-before tx-result)
        task (node->task db :node/completion complete-place)
        n (n-peers db (:db/id task))]
    {:n-peers n :tx (:tx (first (:tx-data tx-result)))}))

