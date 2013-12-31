(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap close! >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.system :as s]
            [onyx.util :as u]))

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq}))

(def components (alter-var-root #'system component/start))

(def coordinator (:coordinator components))
(def log (:log components))

(deftest new-peer
  (let [peer (extensions/create :zookeeper :peer)
        offer-ch-spy (chan 1)]
    (tap (:offer-mult coordinator) offer-ch-spy)
    (>!! (:born-peer-ch-head coordinator) peer)
    (<!! offer-ch-spy)
    (let [query '[:find ?p :where [?e :peer/place ?p]]
          result (d/q query (d/db (:conn log)))]
      (is (= (count result) 1))
      (is (= (ffirst result) peer)))))

(deftest peer-joins-and-dies
  (let [peer (extensions/create :zookeeper :peer)
        offer-ch-spy (chan 1)
        evict-ch-spy (chan 1)]
    (tap (:offer-mult coordinator) offer-ch-spy)
    (tap (:evict-mult coordinator) evict-ch-spy)
    (>!! (:born-peer-ch-head coordinator) peer)
    (<!! offer-ch-spy)
    (extensions/delete :zookeeper peer)
    (<!! evict-ch-spy)
    (let [query '[:find ?p :where [?e :peer/place ?p]]
          result (d/q query (d/db (:conn log)))]
      (is (zero? (count result))))))

(run-tests)

(alter-var-root #'system component/stop)

