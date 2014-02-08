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
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}
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

(def coord-opts {:datomic-uri (str "datomic:mem://" id)
                 :hornetq-addr "localhost:5445"
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 2000})

(def conn (onyx.api/connect (str "onyx:mem//localhost/" id) coord-opts))

(def peer-opts {:hornetq-addr "localhost:5445"
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

(def v-peers (onyx.api/start-peers conn 6 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(try
  (dorun (map deref (map :runner v-peers)))
  (finally
   (doseq [v-peer v-peers]
     (try
       ((:shutdown-fn v-peer))
       (catch Exception e (prn e))))

   (try
     (onyx.api/shutdown conn)
     (catch Exception e (prn e)))))

