(ns onyx.validation-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.test-helper :refer [load-config]]
            [taoensso.timbre :refer [info] :as timbre]
            [clojure.test :refer [deftest is testing]]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config
  (assoc (:env-config config)
         :onyx/id id
         :onyx.log/config {:appenders {:standard-out {:enabled? false}
                                       :spit {:enabled? false}}}))

(def peer-config
  (assoc (:peer-config config)
         :onyx/id id
         :onyx.peer/job-scheduler :onyx.job-scheduler/balanced))

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

(def incomplete-catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/medium :onyx-memory-test-plugin
    :onyx/bootstrap? true
    :onyx/batch-size 2}])

(deftest bad-jobs-1
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
                                                           :task-scheduler :onyx.task-scheduler/balanced}))))

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

(def dupes-workflow
  [[:in :intermediate]
   [:in :intermediate]
   [:intermediate :out]])

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

(deftest bad-jobs-2
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

(def invalid-lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :non-namespaced-calls}])

(def correct-catalog
  [{:onyx/name :in
    :onyx/plugin :a/b
    :onyx/medium :some-medium
    :onyx/type :input
    :onyx/bootstrap? true
    :onyx/batch-size 2}
   {:onyx/name :intermediate
    :onyx/fn :a/fn-path
    :onyx/type :function
    :onyx/batch-size 2}
   {:onyx/name :out
    :onyx/plugin :a/b
    :onyx/medium :some-medium
    :onyx/type :output
    :onyx/batch-size 2}])

(def correct-workflow
  [[:in :intermediate]
   [:intermediate :out]])

(deftest bad-jobs-3 
  (is (thrown? Exception (onyx.api/submit-job peer-config {:catalog correct-catalog
                                                           :workflow correct-workflow
                                                           :lifecycles invalid-lifecycles
                                                           :task-scheduler :onyx.task-scheduler/balanced}))))

(onyx.api/shutdown-env env)

(deftest map-set-workflow
  (is (= (sort (onyx.api/map-set-workflow->workflow {:a #{:b :c}
                                                     :b #{:d}
                                                     :c #{:d :e}}))
         (sort [[:a :b]
                [:a :c]
                [:b :d]
                [:c :d]
                [:c :e]]))))

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
      (is (= (count tasks) 7))
      (is (= (:f (:egress-ids a)) (:id f)))
      (is (= (:c (:egress-ids b)) (:id c)))
      (is (= (:d (:egress-ids c)) (:id d)))
      (is (= (:e (:egress-ids d)) (:id e)))
      (is (= (:f (:egress-ids e)) (:id f)))
      (is (= (:g (:egress-ids f)) (:id g))))))
