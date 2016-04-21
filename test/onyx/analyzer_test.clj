(ns onyx.analyzer-test
  (:require [clojure.test :refer [deftest is]]
            [schema.core :as s]
            [onyx.static.analyzer :as a]
            [onyx.schema :as os]))

(defn validate! [schema v]
  (try
    (s/validate schema v)
    (catch Throwable t
      (a/analyze-error t))))

(deftest schema-errors
  (is (= {:error-type :conditional-failed
          :predicates '[onyx-input-task-type
                        onyx-output-task-type
                        onyx-function-task-type]}
         (validate! os/TaskMap {})))

  (is (= {:error-type :conditional-failed
          :predicates '[onyx-input-task-type
                       onyx-output-task-type
                       onyx-function-task-type]}
         (validate! os/TaskMap {:onyx/type :reader})))

  (is (= {:error-type :missing-required-key
          :missing-key :onyx/plugin}
         (validate! os/TaskMap {:onyx/type :input})))
  
  (is (= {:error-type :conditional-failed
          :predicates ['keyword-namespaced?
                       'keyword?]
          :error-key :onyx/plugin}
         (validate! os/TaskMap {:onyx/type :input
                                :onyx/plugin "Foo"})))

  (is (= {:error-type :missing-required-key
          :missing-key :onyx/medium}
         (validate! os/TaskMap {:onyx/type :input
                                :onyx/plugin :a})))

  (is (= {:error-type :type-error
          :expected-type clojure.lang.Keyword
          :found-type java.lang.Long
          :error-value 42
          :error-key :onyx/medium}
         (validate! os/TaskMap {:onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium 42})))

  (is (= {:error-type :value-predicate-error
          :error-key :onyx/name
          :error-value "hello"
          :predicate 'task-name?}
         (validate! os/TaskMap {:onyx/name "hello"
                                :onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium :core.async})))

  (is (= {:error-type :type-error
          :expected-type java.lang.Integer
          :found-type clojure.lang.Keyword
          :error-value :invalid
          :error-key :onyx/batch-size}
         (validate! os/TaskMap {:onyx/name :hello
                                :onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium :core.async
                                :onyx/batch-size :invalid})))

  (is (= {:error-type :type-error
          :expected-type java.lang.String
          :found-type java.lang.Long
          :error-value 42
          :error-key :onyx/doc}
         (validate! os/TaskMap {:onyx/name :hello
                                :onyx/type :input
                                :onyx/plugin :a
                                :onyx/medium :core.async
                                :onyx/batch-size 50
                                :onyx/doc 42}))))
