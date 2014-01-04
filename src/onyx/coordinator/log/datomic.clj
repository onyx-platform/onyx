(ns onyx.coordinator.log.datomic
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.extensions :as extensions]
            [onyx.util :as u]
            [datomic.api :as d]))

(defn log-schema []
  (let [resource (clojure.java.io/resource "datomic-schema.edn")]
    (read-string (slurp resource))))

(defn schema-set? [conn]
  (seq (d/q '[:find ?e :where [?e :db/ident :peer/place]] (d/db conn))))

(defn start-datomic! [uri schema]
  (d/create-database uri)
  (let [conn (d/connect uri)]
    (when-not (schema-set? conn)
      @(d/transact conn schema))
    conn))

(defrecord Datomic [uri schema]
  component/Lifecycle

  (start [component]
    (prn "Starting Datomic")
    (let [conn (start-datomic! uri schema)]
      (assoc component :conn conn)))

  (stop [component]
    (prn "Stopping Datomic")
    (d/delete-database uri)
    (d/shutdown false)
    component))

(defn datomic [uri schema]
  (map->Datomic {:uri uri :schema schema}))

(defmethod extensions/mark-peer-born Datomic
  [log place]
  (let [tx-data [{:db/id (d/tempid :onyx/log)
                  :peer/place place}]]
    @(d/transact (:conn log) tx-data)))

(defmethod extensions/mark-peer-dead Datomic
  [log place]
  (let [query '[:find ?e :in $ ?place :where [?e :peer/place ?place]]
        entity-id (ffirst (d/q query (d/db (:conn log)) place))]
    @(d/transact (:conn log) [[:db.fn/retractEntity entity-id]])))

(defmethod extensions/mark-offered Datomic
  [log])

(defmethod extensions/plan-job Datomic
  [log tasks])

(defmethod extensions/ack Datomic
  [log task])

(defmethod extensions/evict Datomic
  [log task])

(defmethod extensions/complete Datomic
  [log task])

(defmethod extensions/next-task Datomic
  [log])

