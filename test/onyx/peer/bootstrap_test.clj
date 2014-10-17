(ns onyx.peer.bootstrap-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.api]))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def out-queue (str (java.util.UUID/randomUUID)))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow {:in-bootstrapped {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :hornetq.server/type :embedded
   :hornetq.embedded/config (:configs (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))

(hq-util/create-queue! hq-config out-queue)

(defmethod p-ext/apply-fn [:input :onyx-memory-test-plugin]
  [event] {:onyx.core/results [{:n 42}]})

(def catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/medium :onyx-memory-test-plugin
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :onyx/batch-size 2}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.bootstrap-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 5}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 5}])

(def illegal-catalog ["not" "a" "catalog"])

(def illegal-input-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input 
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :onyx/batch-size 2}])

(def illegal-output-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :output 
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :onyx/batch-size 2}])

(def illegal-transformer-catalog
   [{:onyx/name :inc 
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 5}])

(def illegal-grouper-catalog
   [{:onyx/name :inc 
    :onyx/type :grouper
    :onyx/consumption :concurrent
    :onyx/batch-size 5}])

(def illegal-aggregator-catalog
   [{:onyx/name :inc 
    :onyx/type :aggregator
    :onyx/consumption :concurrent
    :onyx/batch-size 5}])

(def incomplete-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/medium :onyx-memory-test-plugin
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :onyx/batch-size 2}])


(fact (onyx.api/submit-job conn {:catalog illegal-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-input-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-output-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-transformer-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-grouper-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-aggregator-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog incomplete-catalog :workflow workflow}) => (throws Exception))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-util/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact results => [{:n 43} :done])

