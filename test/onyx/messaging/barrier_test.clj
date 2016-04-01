(ns onyx.messaging.barrier-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.messaging.aeron :as a]))

(deftest test-rotate
  (is (= [] (a/rotate [])))
  (is (= [2 3 1] (a/rotate [1 2 3])))
  (is (= [3 1 2] (a/rotate [2 3 1])))
  (is (= [1 2 3] (a/rotate [3 1 2]))))

(deftest test-update-watermarks
  (let [gw {}]
    (is (= {:t1 {:p1 {:high-water-mark 0}}}
           (a/update-global-watermarks gw :t1 :p1 {:n 0} false))))

  (let [gw {:t1 {:p1 {:high-water-mark 5}}}]
    (is (= {:t1 {:p1 {:high-water-mark 6}}}
           (a/update-global-watermarks gw :t1 :p1 {:n 0} false))))

  (let [gw {}]
    (is (= {:t1 {:p1 {:high-water-mark 0
                      :barriers {1 #{}}
                      :barrier-index {1 0}}}}
           (a/update-global-watermarks gw :t1 :p1 {:barrier-epoch 1} true))))

  (let [gw {:t1 {:p1 {:high-water-mark 0
                      :barriers {1 #{}}
                      :barrier-index {1 0}}}}]
    (is (= {:t1 {:p1 {:high-water-mark 1
                      :barriers {1 #{} 2 #{}}
                      :barrier-index {1 0 2 1}}}}
           (a/update-global-watermarks gw :t1 :p1 {:barrier-epoch 2} true))))

  (let [gw {:t1 {:p1 {:high-water-mark 0
                      :barriers {1 #{:p0}}
                      :barrier-index {1 0}}}}]
    (is (= {:t1 {:p1 {:high-water-mark 1
                      :barriers {1 #{:p0} 2 #{}}
                      :barrier-index {1 0 2 1}}}}
           (a/update-global-watermarks gw :t1 :p1 {:barrier-epoch 2} true)))))
