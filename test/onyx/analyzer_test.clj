(ns onyx.analyzer-test
  (:require [clojure.test :refer [deftest is]]
            [schema.core :as s]
            [onyx.static.analyzer :as a]
            [onyx.schema :as os]
            [onyx.api]))

(defn validate! [schema v]
  (try
    (s/validate schema v)
    (catch Throwable t
      (a/analyze-error {} t))))

(deftest workflow-errors
  (is (= {:error-type :value-predicate-error
          :error-key 1
          :error-value "b"
          :predicate 'task-name?
          :path [0 1]}
         (validate! os/Workflow [[:a "b"]])))

  (is (= {:error-type :type-error
          :expected-type java.util.List
          :found-type clojure.lang.Keyword
          :error-key 1
          :error-value :c
          :path [1]}
         (validate! os/Workflow [[:a :b] :c])))

  (is (= {:error-type :constraint-violated
          :predicate 'edge-two-nodes?
          :path [0]}
         (validate! os/Workflow [[:a :b :c]])))

  (is (= {:error-type :constraint-violated
          :predicate 'edge-two-nodes?
          :path nil}
         (validate! os/Workflow [])))

  (is (= {:error-type :constraint-violated
          :predicate 'edge-two-nodes?
          :path nil}
         (validate! os/Workflow []))))

(deftest task-map-errors
  (is (= {:error-type :missing-required-key
          :missing-key :onyx/plugin
          :path [:onyx/plugin]}
         (validate! os/TaskMap {:onyx/type :input})))

  (is (= {:error-type :conditional-failed
          :error-value "Foo"
          :predicates ['keyword-namespaced? 'keyword?]
          :error-key :onyx/plugin
          :path [:onyx/plugin]}
         (validate! os/TaskMap {:onyx/type :input
                                :onyx/plugin "Foo"})))

  (is (= {:error-type :missing-required-key
          :missing-key :onyx/medium
          :path [:onyx/medium]}
         (validate! os/TaskMap {:onyx/type :input
                                :onyx/plugin :a})))

  (is (= {:error-type :type-error
          :expected-type clojure.lang.Keyword
          :found-type java.lang.Long
          :error-value 42
          :error-key :onyx/medium
          :path [:onyx/medium]}
         (validate! os/TaskMap {:onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium 42})))

  (is (= {:error-type :value-predicate-error
          :error-key :onyx/name
          :error-value "hello"
          :predicate 'task-name?
          :path [:onyx/name]}
         (validate! os/TaskMap {:onyx/name "hello"
                                :onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium :core.async})))

  (is (= {:error-type :type-error
          :expected-type java.lang.Integer
          :found-type clojure.lang.Keyword
          :error-value :invalid
          :error-key :onyx/batch-size
          :path [:onyx/batch-size]}
         (validate! os/TaskMap {:onyx/name :hello
                                :onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium :core.async
                                :onyx/batch-size :invalid})))

  (is (= {:error-type :type-error
          :expected-type java.lang.String
          :found-type java.lang.Long
          :error-value 42
          :error-key :onyx/doc
          :path [:onyx/doc]}
         (validate! os/TaskMap {:onyx/name :hello
                                :onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium :core.async
                                :onyx/batch-size 50
                                :onyx/doc 42}))))
