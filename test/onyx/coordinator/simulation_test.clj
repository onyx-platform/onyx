(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap close! >!! <!!]]
            [com.stuartsierra.component :as component]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.util :as u]
            [datomic.api :as d]))

(def system (async/onyx-system {:log :datomic :sync :zookeeper :queue :hornetq}))

(def coordinator (:coordinator (alter-var-root #'system component/start)))

(defn sandbox-db []
  (datomic/start-datomic!
   (str "datomic:mem://" (java.util.UUID/randomUUID))
   (datomic/log-schema)))

(deftest new-peer
  (with-redefs [datomic/datomic-conn (delay (sandbox-db))]
    (let [peer (extensions/create :zookeeper :peer)
          offer-ch-spy (chan 1)]
      (tap (:offer-mult coordinator) offer-ch-spy)
      (>!! (:born-peer-ch-head coordinator) peer)
      (<!! offer-ch-spy)
      (let [query '[:find ?p :where [?e :peer/place ?p]]
            result (d/q query (d/db @datomic/datomic-conn))]
        (is (= (count result) 1))
        (is (= (ffirst result) peer))
        (close! offer-ch-spy)))))

(deftest peer-joins-and-dies
  (with-redefs [datomic/datomic-conn (delay (sandbox-db))]
    (let [peer (extensions/create :zookeeper :peer)
          offer-ch-spy (chan 1)
          evict-ch-spy (chan 1)]
      (tap async/offer-mult offer-ch-spy)
      (tap async/evict-mult evict-ch-spy)
      (>!! async/born-peer-ch-head peer)
      (<!! offer-ch-spy)
      (extensions/delete :zookeeper peer)
      (<!! evict-ch-spy)
      (let [query '[:find ?p :where [?e :peer/place ?p]]
            result (d/q query (d/db @datomic/datomic-conn))]
        (is (zero? (count result)))))))

(run-tests)

(alter-var-root #'system component/stop)

