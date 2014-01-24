(ns onyx.coordinator.sim-test-utils
  (:require [com.stuartsierra.component :as component]
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

