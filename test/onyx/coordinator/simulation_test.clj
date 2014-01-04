(ns onyx.coordinator.simulation-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan tap >!! <!!]]
            [com.stuartsierra.component :as component]
            [datomic.api :as d]
            [onyx.coordinator.async :as async]
            [onyx.coordinator.extensions :as extensions]
            [onyx.coordinator.log.datomic :as datomic]
            [onyx.system :as s]
            [onyx.util :as u]))

(def system (s/onyx-system {:sync :zookeeper :queue :hornetq}))

(defn with-system [f]
  (let [components (alter-var-root #'system component/start)
        coordinator (:coordinator components)
        sync (:sync components)
        log (:log components)]
    (f coordinator sync log)
    (alter-var-root #'system component/stop)))

(deftest new-peer
  (with-system
    (fn [coordinator sync log]
      (let [peer (extensions/create sync :peer)
            offer-ch-spy (chan 1)]
        (tap (:offer-mult coordinator) offer-ch-spy)
        (>!! (:born-peer-ch-head coordinator) peer)
        (<!! offer-ch-spy)
        (let [query '[:find ?p :where [?e :peer/place ?p]]
              result (d/q query (d/db (:conn log)))]
          (is (= (count result) 1))
          (is (= (ffirst result) peer)))))))

(deftest peer-joins-and-dies
  (with-system
    (fn [coordinator sync log]
      (let [peer (extensions/create sync :peer)
            offer-ch-spy (chan 1)
            evict-ch-spy (chan 1)]
        (tap (:offer-mult coordinator) offer-ch-spy)
        (tap (:evict-mult coordinator) evict-ch-spy)
        (>!! (:born-peer-ch-head coordinator) peer)
        (<!! offer-ch-spy)
        (extensions/delete sync peer)
        (<!! evict-ch-spy)
        (let [query '[:find ?p :where [?e :peer/place ?p]]
              result (d/q query (d/db (:conn log)))]
          (is (zero? (count result))))))))

(deftest plan-one-job-no-peers
  (with-system
    (fn [coordinator sync log]
      (let [catalog [{:onyx/name :in
                      :onyx/direction :input
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :hornetq/queue-name "in-queue"}
                     {:onyx/name :inc
                      :onyx/type :transformer}
                     {:onyx/name :out
                      :onyx/direction :output
                      :onyx/type :queue
                      :onyx/medium :hornetq
                      :hornetq/queue-name "out-queue"}]
            workflow {:in {:inc :out}}
            offer-ch-spy (chan 1)]
        (tap (:offer-mult coordinator) offer-ch-spy)
        (>!! (:planning-ch-head coordinator)
             {:catalog catalog :workflow workflow})
        (<!! offer-ch-spy)))))

(run-tests 'onyx.coordinator.simulation-test)

