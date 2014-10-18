(ns onyx.validation-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.coordinator.planning :refer [unpack-map-workflow]]
            [onyx.api]))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

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

(def conn (onyx.api/connect :memory coord-opts))

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

(fact (unpack-map-workflow {:a :b}) => [[:a :b]])
(fact (unpack-map-workflow {:a {:b :c}}) => [[:a :b] [:b :c]])
(fact (unpack-map-workflow {:a {:b {:c :d}}}) => [[:a :b] [:b :c] [:c :d]])
(fact (unpack-map-workflow {:a {:b :c :d :e}}) => [[:a :b] [:a :d] [:b :c] [:d :e]])

(fact (into #{} (unpack-map-workflow {:a {:b :c} :d {:e :f :g :h}}))
      => #{[:a :b]
           [:b :c]
           [:d :e]
           [:d :g]
           [:e :f]
           [:g :h]})

