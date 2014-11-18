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

(def illegal-function-catalog
  [{:onyx/name :inc
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 5}])

(def illegal-dispatch-catalog
  [{:onyx/name :input
    :onyx/type :input
    :onyx/medium :core.async
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

(fact (onyx.api/submit-job conn {:catalog illegal-function-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-dispatch-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-grouper-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog illegal-aggregator-catalog :workflow workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog incomplete-catalog :workflow workflow}) => (throws Exception))

(def workflow-tests-catalog
  [{:onyx/name :in
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size 5}
   {:onyx/name :intermediate
    :onyx/fn :test-fn
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 5}
   {:onyx/name :out
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/consumption :concurrent
    :onyx/batch-size 5}])

(def illegal-incoming-inputs-workflow 
  [[:intermediate :in]])

(def illegal-outgoing-outputs-workflow 
  [[:out :intermediate]])

(def illegal-edge-nodes-count-workflow 
  [[:in :intermediate]
   [:intermediate]])

(def illegal-intermediate-nodes-workflow 
  [[:in :intermediate]
   [:in :out]])

(fact (onyx.api/submit-job conn {:catalog workflow-tests-catalog :workflow illegal-incoming-inputs-workflow}) => (throws Exception))
(fact (onyx.api/submit-job conn {:catalog workflow-tests-catalog :workflow illegal-outgoing-outputs-workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog workflow-tests-catalog :workflow illegal-edge-nodes-count-workflow}) => (throws Exception))

(fact (onyx.api/submit-job conn {:catalog workflow-tests-catalog :workflow illegal-intermediate-nodes-workflow}) => (throws Exception))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact (unpack-map-workflow {:a :b}) => [[:a :b]])
(fact (unpack-map-workflow {:a {:b :c}}) => [[:a :b] [:b :c]])
(fact (unpack-map-workflow {:a {:b {:c :d}}}) => [[:a :b] [:b :c] [:c :d]])
(fact (unpack-map-workflow {:a {:b :c :d :e}}) => [[:a :b] [:a :d] [:b :c] [:d :e]])


(fact (sort (onyx.api/map-set-workflow->workflow {:a #{:b :c}
                                                  :b #{:d}
                                                  :c #{:d :e}}))
                                           =>
                                           (sort [[:a :b]
                                                  [:a :c]
                                                  [:b :d]
                                                  [:c :d]
                                                  [:c :e]]))


(fact (into #{} (unpack-map-workflow {:a {:b :c} :d {:e :f :g :h}}))
      => #{[:a :b]
           [:b :c]
           [:d :e]
           [:d :g]
           [:e :f]
           [:g :h]})

(let [catalog
      [{:onyx/name :a
        :onyx/type :input
        :onyx/medium :hornetq
        :onyx/consumption :concurrent}

       {:onyx/name :b
        :onyx/type :input
        :onyx/consumption :concurrent}

       {:onyx/name :c
        :onyx/type :function
        :onyx/consumption :concurrent}

       {:onyx/name :d
        :onyx/type :function
        :onyx/consumption :concurrent}

       {:onyx/name :e
        :onyx/type :function
        :onyx/consumption :concurrent}

       {:onyx/name :f
        :onyx/type :function
        :onyx/consumption :concurrent}

       {:onyx/name :g
        :onyx/type :output
        :onyx/medium :hornetq
        :onyx/consumption :concurrent}]
      workflow [[:a :f] [:b :c] [:c :d] [:d :e] [:e :f] [:f :g]]
      tasks (onyx.coordinator.planning/discover-tasks catalog workflow)

      [a b c d e f g :as sorted-tasks]
      (reduce (fn [all next]
                (conj all (first (filter #(= (:name %) next) tasks))))
              [] [:a :b :c :d :e :f :g])]

  (fact "There are 7 tasks"
        (count tasks) => 7)

  (fact "The tasks are topologically sorted into phases"
        (map :phase sorted-tasks) => (range 7))

  (fact (:f (:egress-queues a)) => (:a (:ingress-queues f)))
  (fact (:c (:egress-queues b)) => (:b (:ingress-queues c)))
  (fact (:d (:egress-queues c)) => (:c (:ingress-queues d)))
  (fact (:e (:egress-queues d)) => (:d (:ingress-queues e)))
  (fact (:f (:egress-queues e)) => (:e (:ingress-queues f)))
  (fact (:g (:egress-queues f)) => (:f (:ingress-queues g)))

  (fact ":a has an ingress queue" (:ingress-queues a) =not=> nil?)
  (fact ":b has an ingress queue" (:ingress-queues b) =not=> nil?)
  (fact ":g has an egress queue" (:egress-queues g) =not=> empty?))

