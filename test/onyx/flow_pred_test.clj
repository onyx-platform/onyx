(ns onyx.flow-pred-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.static.planning :refer [build-pred-fn]]))

(def true-pred (constantly true))
(def false-pred (constantly false))

(deftest flow-pred
  (is (true? ((build-pred-fn :onyx.flow-pred-test/true-pred {}) [1 2 3])))

  (is (false? ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                               :onyx.flow-pred-test/false-pred] {}) [5 6 7])))

  (is (true? ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                              :onyx.flow-pred-test/true-pred] {}) [5 6 7])))

  (is (false? ((build-pred-fn [:and :onyx.flow-pred-test/false-pred
                               [:and :onyx.flow-pred-test/true-pred
                                :onyx.flow-pred-test/true-pred]] {}) [5])))

  (is (false? ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                               [:and :onyx.flow-pred-test/true-pred
                                :onyx.flow-pred-test/false-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:or :onyx.flow-pred-test/true-pred
                              :onyx.flow-pred-test/false-pred] {}) [5])))

  (is (nil? ((build-pred-fn [:or :onyx.flow-pred-test/false-pred
                             :onyx.flow-pred-test/false-pred] {}) [5])))

  (is (true? ((build-pred-fn [:or :onyx.flow-pred-test/true-pred
                              [:or :onyx.flow-pred-test/true-pred
                               :onyx.flow-pred-test/false-pred]] {}) [5])))

  (is (false? ((build-pred-fn [:not :onyx.flow-pred-test/true-pred] {}) [1 2 3])))

  (is (true? ((build-pred-fn [:not :onyx.flow-pred-test/false-pred] {}) [5])))

  (is (false? ((build-pred-fn [:not [:not :onyx.flow-pred-test/false-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:not [:not :onyx.flow-pred-test/true-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:or :onyx.flow-pred-test/true-pred
                              [:and :onyx.flow-pred-test/false-pred
                               :onyx.flow-pred-test/false-pred]] {}) [5])))

  (is (nil? ((build-pred-fn [:or [:not :onyx.flow-pred-test/true-pred]
                             [:and :onyx.flow-pred-test/false-pred
                              :onyx.flow-pred-test/false-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                              [:and :onyx.flow-pred-test/true-pred
                               :onyx.flow-pred-test/true-pred]] {}) [1 2 3])))) 

