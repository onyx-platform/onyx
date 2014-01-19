(ns onyx.coordinator.coordinator-simulant-tests
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [simulant.sim :as sim]
            [simulant.util :as u]
            [datomic.api :as d]
            [onyx.system :as system]
            [onyx.coordinator.log.datomic :as datomic]))

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

(def sim-uri (str "datomic:mem://" (d/squuid)))

(def sim-conn (reset-conn sim-uri))


;; Simulant generic schema
(load-schema sim-conn "simulant/schema.edn")

;; Simulant schema for coordinator sim
(load-schema sim-conn "simulant/coordinator-sim.edn")



