(ns onyx.validation-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.test-helper :refer [load-config with-test-env]]
            [taoensso.timbre :refer [info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.schema :as os]
            [schema.core :as s]
            [onyx.api]))

(deftest validation-errors
  (let [id (java.util.UUID/randomUUID)
        config (load-config)
        env-config
        (assoc (:env-config config)
               :onyx/tenancy-id id
               :onyx.log/config {:appenders {:standard-out {:enabled? false}
                                             :spit {:enabled? false}}})
        peer-config
        (assoc (:peer-config config)
               :onyx/tenancy-id id
               :onyx.peer/job-scheduler :onyx.job-scheduler/balanced)
        env (onyx.api/start-env env-config)
        workflow
        [[:in-bootstrapped :inc]
         [:inc :out]]
        illegal-catalog ["not" "a" "catalog"]
        illegal-input-catalog
        [{:onyx/name :in-bootstrapped
          :onyx/type :input
          :onyx/batch-size 2}]
        illegal-output-catalog
        [{:onyx/name :in-bootstrapped
          :onyx/type :output
          :onyx/batch-size 2}]

        illegal-function-catalog
        [{:onyx/name :inc
          :onyx/type :function
          :onyx/batch-size 5}]

        illegal-dispatch-catalog
        [{:onyx/name :input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/batch-size 5}]

        incomplete-catalog
        [{:onyx/name :in-bootstrapped
          :onyx/type :input
          :onyx/medium :onyx-memory-test-plugin
          :onyx/batch-size 2}]

        workflow-tests-catalog
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
          :onyx/batch-size 5}]

        dupes-workflow
        [[:in :intermediate]
         [:in :intermediate]
         [:intermediate :out]]

        illegal-incoming-inputs-workflow
        [[:intermediate :in]]

        illegal-outgoing-outputs-workflow
        [[:out :intermediate]]

        illegal-edge-nodes-count-workflow
        [[:in :intermediate]
         [:intermediate]]

        illegal-intermediate-nodes-workflow
        [[:in :intermediate]
         [:in :out]]

        invalid-lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls :non-namespaced-calls}]

        bad-fn-ns-form
        [{:onyx/name :in
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :input
          :onyx/batch-size 2}
         {:onyx/name :intermediate
          :onyx/fn :fn-path
          :onyx/type :function
          :onyx/batch-size 2}
         {:onyx/name :out
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :output
          :onyx/batch-size 2}]

        bad-input-plugin
        [{:onyx/name :in
          :onyx/plugin :ab
          :onyx/medium :some-medium
          :onyx/type :input
          :onyx/batch-size 2}
         {:onyx/name :intermediate
          :onyx/fn :a/fn-path
          :onyx/type :function
          :onyx/batch-size 2}
         {:onyx/name :out
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :output
          :onyx/batch-size 2}]

        bad-output-plugin
        [{:onyx/name :in
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :input
          :onyx/batch-size 2}
         {:onyx/name :intermediate
          :onyx/fn :a/fn-path
          :onyx/type :function
          :onyx/batch-size 2}
         {:onyx/name :out
          :onyx/plugin :b
          :onyx/medium :some-medium
          :onyx/type :output
          :onyx/batch-size 2}]

        java-input-plugin
        [{:onyx/name :in
          :onyx/plugin :ab
          :onyx/medium :some-medium
          :onyx/language :java
          :onyx/type :input
          :onyx/batch-size 2}
         {:onyx/name :intermediate
          :onyx/fn :a/fn-path
          :onyx/type :function
          :onyx/batch-size 2}
         {:onyx/name :out
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :output
          :onyx/batch-size 2}]

        correct-catalog
        [{:onyx/name :in
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :input
          :onyx/batch-size 2}
         {:onyx/name :intermediate
          :onyx/fn :a/fn-path
          :onyx/type :function
          :onyx/batch-size 2}
         {:onyx/name :out
          :onyx/plugin :a/b
          :onyx/medium :some-medium
          :onyx/type :output
          :onyx/batch-size 2}]

        correct-workflow [[:in :intermediate] [:intermediate :out]]]
    (testing "bad-jobs-1"
      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog illegal-catalog :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog illegal-input-catalog :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog illegal-output-catalog :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog illegal-function-catalog :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog illegal-dispatch-catalog :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog incomplete-catalog :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog bad-fn-ns-form :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog bad-input-plugin :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog bad-output-plugin :workflow workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced}))))

    (testing "bad-jobs-2"
      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                                               :workflow illegal-incoming-inputs-workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                                               :workflow illegal-outgoing-outputs-workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                                               :workflow illegal-edge-nodes-count-workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                                               :workflow illegal-intermediate-nodes-workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced})))

      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog workflow-tests-catalog
                                                               :workflow dupes-workflow
                                                               :task-scheduler :onyx.task-scheduler/balanced}))))

    (testing "bad-jobs-3" 
      (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog correct-catalog
                                                               :workflow correct-workflow
                                                               :lifecycles invalid-lifecycles
                                                               :task-scheduler :onyx.task-scheduler/balanced}))))

    (onyx.api/shutdown-env env)))


(deftest map-set-workflow
  (is (= (sort [[:a :b]
                [:a :c]
                [:b :d]
                [:c :d]
                [:c :e]])
         (sort (onyx.api/map-set-workflow->workflow {:a #{:b :c}
                                                     :b #{:d}
                                                     :c #{:d :e}})))))

(deftest task-discovery
  (let [catalog
        [{:onyx/name :a
          :onyx/type :input
          :onyx/medium :core.async}

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
          :onyx/medium :core.async}]
        workflow [[:a :f] [:b :c] [:c :d] [:d :e] [:e :f] [:f :g]]
        tasks (onyx.static.planning/discover-tasks catalog workflow)

        [a b c d e f g :as sorted-tasks]
        (reduce (fn [all next]
                  (conj all (first (filter #(= (:name %) next) tasks))))
                [] [:a :b :c :d :e :f :g])]

    (testing "There are 7 tasks"
      (is (= 7 (count tasks)))
      (is (= (:id f) (:f (:egress-ids a))))
      (is (= (:id c) (:c (:egress-ids b))))
      (is (= (:id d) (:d (:egress-ids c))))
      (is (= (:id e) (:e (:egress-ids d))))
      (is (= (:id f) (:f (:egress-ids e))))
      (is (= (:id g) (:g (:egress-ids f)))))))

(deftest task-map-schemas 
  (testing "Input examples"
    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/plugin :your-plugin/builder
                     :onyx/medium :some-medium
                     :onyx/type :output
                     :onyx/min-peers 2
                     :onyx/batch-size 40}))

    (is (thrown? Exception
                 (s/validate os/InputTaskSchema 
                             {:onyx/name :sum-balance
                              :onyx/plugin :your-plugin/builder
                              :onyx/medium :some-medium
                              :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                              :onyx/type :input
                              :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
                              :onyx/min-peers 2
                              :onyx/flux-policy :kill
                              :onyx/batch-size 40})))

    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/plugin :your-java-plugin-ns
                     :onyx/language :java
                     :onyx/medium :some-medium
                     :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                     :onyx/type :input
                     :onyx/min-peers 2
                     :onyx/batch-size 40}))

    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/plugin :your-java-plugin-ns
                     :onyx/language :java
                     :onyx/medium :some-medium
                     :onyx/fn :some/fn
                     :onyx/type :input
                     :onyx/min-peers 2
                     :onyx/batch-size 40})))

  (testing "Function examples"
    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                     :onyx/type :function
                     :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
                     :onyx/min-peers 2
                     :onyx/flux-policy :kill
                     :onyx/batch-size 40}))
    
    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                     :onyx/type :function
                     :onyx/min-peers 2
                     :onyx/batch-size 40}))
    
    (is (thrown? Exception 
                 (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                     :onyx/type :function
                     :onyx/group-by-fn :a/b
                     :onyx/min-peers 2
                     :onyx/batch-size 40})))

    (is (thrown? Exception 
                 (s/validate os/TaskMap 
                             {:onyx/name :sum-balance
                              :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                              :onyx/type :function
                              :onyx/group-by-fn :a/b
                              :onyx/flux-policy :recover
                              :onyx/batch-size 40})))

    (is (s/validate os/TaskMap 
                             {:onyx/name :sum-balance
                              :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                              :onyx/type :function
                              :onyx/group-by-fn :a/b
                              :onyx/min-peers 2
                              :onyx/flux-policy :kill
                              :onyx/batch-size 40}))
    
    (is (s/validate os/TaskMap 
                             {:onyx/name :sum-balance
                              :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                              :onyx/type :function
                              :onyx/group-by-fn :a/b
                              :onyx/max-peers 2
                              :onyx/min-peers 2
                              :onyx/flux-policy :recover
                              :onyx/batch-size 40})))

  (testing "Output examples"
    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/plugin :your-plugin/builder
                     :onyx/medium :some-medium
                     :onyx/type :output
                     :onyx/min-peers 2
                     :onyx/batch-size 40}))

    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/plugin :your-plugin/builder
                     :onyx/medium :some-medium
                     :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                     :onyx/type :output
                     :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
                     :onyx/min-peers 2
                     :onyx/flux-policy :kill
                     :onyx/batch-size 40}))

    (is (s/validate os/TaskMap 
                    {:onyx/name :sum-balance
                     :onyx/plugin :your-plugin
                     :onyx/language :java
                     :onyx/medium :some-medium
                     :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                     :onyx/type :output
                     :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
                     :onyx/min-peers 2
                     :onyx/flux-policy :kill
                     :onyx/batch-size 40}))

    (is (thrown? Exception 
                 (s/validate os/TaskMap 
                             {:onyx/name :sum-balance
                              :onyx/plugin :your-plugin/bad-plugin
                              :onyx/language :java
                              :onyx/medium :some-medium
                              :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                              :onyx/type :output
                              :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
                              :onyx/min-peers 2
                              :onyx/flux-policy :kill
                              :onyx/batch-size 40})))

    (is (thrown? Exception 
                 (s/validate os/TaskMap 
                             {:onyx/name :sum-balance
                              :onyx/plugin :your-plugin
                              :onyx/language :clojure
                              :onyx/medium :some-medium
                              :onyx/fn :onyx.peer.fn-grouping-test/sum-balance
                              :onyx/type :output
                              :onyx/group-by-fn :onyx.peer.fn-grouping-test/group-by-name
                              :onyx/min-peers 2
                              :onyx/flux-policy :kill
                              :onyx/batch-size 40})))))

(deftest java-style-functions
  (testing "Non-namespaced keywords are used for Java entries"
    (is
     (s/validate os/FunctionTaskSchema
                 {:onyx/name :my-task
                  :onyx/language :java
                  :onyx/fn :my.class
                  :onyx/type :function
                  :onyx/batch-size 40}))

    (is
     (s/validate os/InputTaskSchema
                 {:onyx/name :my-task
                  :onyx/language :java
                  :onyx/fn :my.class
                  :onyx/plugin :my.other.class
                  :onyx/type :input
                  :onyx/medium :abc
                  :onyx/batch-size 40}))

    (is
     (s/validate os/OutputTaskSchema
                 {:onyx/name :my-task
                  :onyx/language :java
                  :onyx/fn :my.class
                  :onyx/plugin :my.other.class
                  :onyx/type :output
                  :onyx/medium :abc
                  :onyx/batch-size 40}))

    (is
     (s/validate os/FunctionTaskSchema
                 {:onyx/name :my-task
                  :onyx/language :java
                  :onyx/fn :my.class
                  :onyx/type :function
                  :onyx/batch-size 40}))))

(deftest keyword-namespace-restriction
  (testing "build-allowed-key-ns throws exception only on restriction"
    (is (thrown? Exception
                 (s/validate {:myplugin/option s/Str
                              (os/build-allowed-key-ns :myplugin) s/Any}
                             {:myplugin/option "chocolate"
                              :myplugin/extra-option "vanilla"})))
    (is (s/validate {:myplugin/option s/Str
                     (os/build-allowed-key-ns :myplugin) s/Any}
                    {:myplugin/option "chocolate"
                     :otherplugin/extra-option "vanilla"})))
  (testing "restricted-ns throws exception only on restriction"
    (is (thrown? Exception
                 (s/validate {:myplugin/option s/Str
                              (os/restricted-ns :myplugin) s/Any}
                             {:myplugin/option "chocolate"
                              :myplugin/extra-option "vanilla"})))
    (is (s/validate {:myplugin/option s/Str
                     (os/restricted-ns :myplugin) s/Any}
                    {:myplugin/option "chocolate"
                     :otherplugin/extra-option "vanilla"})))
  (testing "s/explain on restricted-ns returns something readable"
    (is (= (first (remove nil? (map (fn [x] (if (vector? x) (second x) x))
                                    (keys (s/explain {(os/restricted-ns :myplugin) s/Any
                                                      :myplugin/option s/Str})))))
           :myplugin))))
