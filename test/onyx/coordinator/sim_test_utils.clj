(ns onyx.coordinator.sim-test-utils
  (:require [com.stuartsierra.component :as component]
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

