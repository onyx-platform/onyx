(ns onyx.messaging.barrier-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.messaging.aeron :refer [rotate]]))

(deftest test-rotate
  (is (= [] (rotate [])))
  (is (= [2 3 1] (rotate [1 2 3])))
  (is (= [3 1 2] (rotate [2 3 1])))
  (is (= [1 2 3] (rotate [3 1 2]))))
