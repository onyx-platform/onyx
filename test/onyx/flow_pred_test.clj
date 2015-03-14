(ns onyx.flow-pred-test
  (:require [midje.sweet :refer :all]
            [onyx.static.planning :refer [build-pred-fn]]))

(def true-pred (constantly true))

(def false-pred (constantly false))

(fact ((build-pred-fn :onyx.flow-pred-test/true-pred {}) [1 2 3]) => true)

(fact ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                       :onyx.flow-pred-test/false-pred] {}) [5 6 7]) => false)

(fact ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                       :onyx.flow-pred-test/true-pred] {}) [5 6 7]) => true)

(fact ((build-pred-fn [:and :onyx.flow-pred-test/false-pred
                       [:and :onyx.flow-pred-test/true-pred
                        :onyx.flow-pred-test/true-pred]] {}) [5]) => false)

(fact ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                       [:and :onyx.flow-pred-test/true-pred
                        :onyx.flow-pred-test/false-pred]] {}) [5]) => false)

(fact ((build-pred-fn [:or :onyx.flow-pred-test/true-pred
                       :onyx.flow-pred-test/false-pred] {}) [5]) => true)

(fact ((build-pred-fn [:or :onyx.flow-pred-test/false-pred
                       :onyx.flow-pred-test/false-pred] {}) [5]) => nil)

(fact ((build-pred-fn [:or :onyx.flow-pred-test/true-pred
                       [:or :onyx.flow-pred-test/true-pred
                        :onyx.flow-pred-test/false-pred]] {}) [5]) => true)

(fact ((build-pred-fn [:not :onyx.flow-pred-test/true-pred] {}) [1 2 3]) => false)

(fact ((build-pred-fn [:not :onyx.flow-pred-test/false-pred] {}) [5]) => true)

(fact ((build-pred-fn [:not [:not :onyx.flow-pred-test/false-pred]] {}) [5]) => false)

(fact ((build-pred-fn [:not [:not :onyx.flow-pred-test/true-pred]] {}) [5]) => true)

(fact ((build-pred-fn [:or :onyx.flow-pred-test/true-pred
                       [:and :onyx.flow-pred-test/false-pred
                        :onyx.flow-pred-test/false-pred]] {}) [5]) => true)

(fact ((build-pred-fn [:or [:not :onyx.flow-pred-test/true-pred]
                       [:and :onyx.flow-pred-test/false-pred
                        :onyx.flow-pred-test/false-pred]] {}) [5]) => nil)

(fact ((build-pred-fn [:and :onyx.flow-pred-test/true-pred
                       [:and :onyx.flow-pred-test/true-pred
                        :onyx.flow-pred-test/true-pred]] {}) [1 2 3]) => true)

