(ns onyx.validation-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id id
    :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def env (onyx.api/start-env env-config))

(def workflow
  [[:in-bootstrapped :inc]
   [:inc :out]])

(def illegal-catalog ["not" "a" "catalog"])

(def illegal-input-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/bootstrap? true
    :onyx/batch-size 2}])

(def illegal-output-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :output
    :onyx/bootstrap? true
    :onyx/batch-size 2}])

(def illegal-function-catalog
  [{:onyx/name :inc
    :onyx/type :function
    :onyx/batch-size 5}])

(def illegal-dispatch-catalog
  [{:onyx/name :input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 5}])

(def illegal-grouper-catalog
  [{:onyx/name :inc
    :onyx/type :grouper
    :onyx/batch-size 5}])

(def illegal-aggregator-catalog
  [{:onyx/name :inc
    :onyx/type :aggregator
    :onyx/batch-size 5}])

(def incomplete-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/medium :onyx-memory-test-plugin
    :onyx/bootstrap? true
    :onyx/batch-size 2}])

(fact (onyx.api/submit-job peer-config {:catalog illegal-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog illegal-input-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog illegal-output-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog illegal-function-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog illegal-dispatch-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog illegal-grouper-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog illegal-aggregator-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog incomplete-catalog :workflow workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin}) => (throws Exception))

(def workflow-tests-catalog
  [{:onyx/name :in
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 5}
   {:onyx/name :intermediate
    :onyx/fn :test-fn
    :onyx/type :function
    :onyx/batch-size 5}
   {:onyx/name :out
    :onyx/type :output
    :onyx/medium :core.async
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

(fact (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                        :workflow illegal-incoming-inputs-workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin})
      => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                        :workflow illegal-outgoing-outputs-workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin})
      => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                        :workflow illegal-edge-nodes-count-workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin})
      => (throws Exception))

(fact (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                        :workflow illegal-intermediate-nodes-workflow
                                        :task-scheduler :onyx.task-scheduler/round-robin})
      => (throws Exception))

(onyx.api/shutdown-env env)

(fact (onyx.api/unpack-map-workflow {:a :b}) => [[:a :b]])
(fact (onyx.api/unpack-map-workflow {:a {:b :c}}) => [[:a :b] [:b :c]])
(fact (onyx.api/unpack-map-workflow {:a {:b {:c :d}}}) => [[:a :b] [:b :c] [:c :d]])
(fact (onyx.api/unpack-map-workflow {:a {:b :c :d :e}}) => [[:a :b] [:a :d] [:b :c] [:d :e]])

(fact (sort (onyx.api/map-set-workflow->workflow {:a #{:b :c}
                                                  :b #{:d}
                                                  :c #{:d :e}}))
      =>
      (sort [[:a :b]
             [:a :c]
             [:b :d]
             [:c :d]
             [:c :e]]))


(fact (into #{} (onyx.api/unpack-map-workflow {:a {:b :c} :d {:e :f :g :h}}))
      => #{[:a :b]
           [:b :c]
           [:d :e]
           [:d :g]
           [:e :f]
           [:g :h]})

(let [catalog
      [{:onyx/name :a
        :onyx/type :input
        :onyx/medium :hornetq}

       {:onyx/name :b
        :onyx/type :input}

       {:onyx/name :c
        :onyx/type :function}

       {:onyx/name :d
        :onyx/type :function}

       {:onyx/name :e
        :onyx/type :function}

       {:onyx/name :f
        :onyx/type :function}

       {:onyx/name :g
        :onyx/type :output
        :onyx/medium :hornetq}]
      workflow [[:a :f] [:b :c] [:c :d] [:d :e] [:e :f] [:f :g]]
      tasks (onyx.static.planning/discover-tasks catalog workflow)

      [a b c d e f g :as sorted-tasks]
      (reduce (fn [all next]
                (conj all (first (filter #(= (:name %) next) tasks))))
              [] [:a :b :c :d :e :f :g])]

  (fact "There are 7 tasks"
        (count tasks) => 7)

  (fact (:f (:egress-ids a)) => (:id f))
  (fact (:c (:egress-ids b)) => (:id c))
  (fact (:d (:egress-ids c)) => (:id d))
  (fact (:e (:egress-ids d)) => (:id e))
  (fact (:f (:egress-ids e)) => (:id f))
  (fact (:g (:egress-ids f)) => (:id g)))

