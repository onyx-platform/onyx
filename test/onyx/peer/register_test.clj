(ns onyx.coordinator.single-peer-test
  (:require [midje.sweet :refer :all]
            [onyx.api]))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/direction :input
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name "in-queue"
    :hornetq/host "localhost"
    :hornetq/port 5445}
   {:onyx/name :inc
    :onyx/fn :onyx.coordinator.single-peer-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent}
   {:onyx/name :out
    :onyx/direction :output
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name "out-queue"
    :hornetq/host "localhost"
    :hornetq/port 5445}])

(def workflow {:in {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def opts {:revoke-delay 3000 :onyx-id id})

(def coordinator-conn (onyx.api/connect (str "onyx:mem//localhost/" id) opts))

(def v-peers (onyx.api/start-peers coordinator-conn 2 {:onyx-id (:onyx-id opts)}))

(onyx.api/submit-job catalog workflow)

(dorun (map deref (map :runner v-peers)))
