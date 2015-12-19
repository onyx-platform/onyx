(ns onyx.flow-conditions.flow-pred-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.flow-conditions.fc-compile :refer [build-pred-fn]]))

(def true-pred (constantly true))
(def false-pred (constantly false))

(deftest flow-pred
  (is (true? ((build-pred-fn ::true-pred {}) [1 2 3])))

  (is (false? ((build-pred-fn [:and ::true-pred ::false-pred] {}) [5 6 7])))

  (is (true? ((build-pred-fn [:and ::true-pred ::true-pred] {}) [5 6 7])))

  (is (false? ((build-pred-fn [:and ::false-pred
                               [:and ::true-pred ::true-pred]] {}) [5])))

  (is (false? ((build-pred-fn [:and ::true-pred
                               [:and ::true-pred ::false-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:or ::true-pred ::false-pred] {}) [5])))

  (is (nil? ((build-pred-fn [:or ::false-pred ::false-pred] {}) [5])))

  (is (true? ((build-pred-fn [:or ::true-pred
                              [:or ::true-pred ::false-pred]] {}) [5])))

  (is (false? ((build-pred-fn [:not ::true-pred] {}) [1 2 3])))

  (is (true? ((build-pred-fn [:not ::false-pred] {}) [5])))

  (is (false? ((build-pred-fn [:not [:not ::false-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:not [:not ::true-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:or ::true-pred
                              [:and ::false-pred ::false-pred]] {}) [5])))

  (is (nil? ((build-pred-fn [:or [:not ::true-pred]
                             [:and ::false-pred ::false-pred]] {}) [5])))

  (is (true? ((build-pred-fn [:and ::true-pred
                              [:and ::true-pred ::true-pred]] {}) [1 2 3]))))

