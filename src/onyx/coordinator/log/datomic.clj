(ns onyx.coordinator.log.datomic
  (:require [datomic.api :as d]
            [onyx.coordinator.extensions :as extensions]
            [onyx.util :as u]))

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

(def datomic-conn
  (delay (start-datomic! (:datomic-uri (u/config)) (log-schema))))

(defmethod extensions/mark-peer-born :datomic
  [log place]
  (let [tx-data [{:db/id (d/tempid :onyx/log)
                  :peer/place place}]]
    @(d/transact @datomic-conn tx-data)))

(defmethod extensions/mark-peer-dead :datomic
  [log place]
  (let [query '[:find ?e :in $ ?place :where [?e :peer/place ?place]]
        entity-id (ffirst (d/q query (d/db @datomic-conn) place))]
    @(d/transact @datomic-conn [[:db.fn/retractEntity entity-id]])))

(defmethod extensions/mark-offered :datomic
  [log])

(defmethod extensions/plan-job :datomic
  [log job])

(defmethod extensions/ack :datomic
  [log task])

(defmethod extensions/evict :datomic
  [log task])

(defmethod extensions/complete :datomic
  [log task])

(defmethod extensions/next-task :datomic
  [log])

