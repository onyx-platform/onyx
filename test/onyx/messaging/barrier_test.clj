(ns onyx.messaging.barrier-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.messaging.aeron :as a]
            [onyx.api]))

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

(deftest test-calculate-ticket
  (is (= -1 (a/calculate-ticket {} :p1 2)))

  (let [gw {:high-water-mark 5}]
    (is (= [0 1] (a/calculate-ticket gw :p1 2))))

  (let [gw {:high-water-mark 5}]
    (is (= [0 3] (a/calculate-ticket gw :p1 4))))

  (let [gw {:high-water-mark 5}]
    (is (= [0 4] (a/calculate-ticket gw :p1 5))))

  (let [gw {:high-water-mark 5}]
    (is (= [0 5] (a/calculate-ticket gw :p1 6))))

  (let [gw {:high-water-mark 5}]
    (is (= [0 5] (a/calculate-ticket gw :p1 7))))

  (let [gw {:low-water-mark 0
            :high-water-mark 5}]
    (is (= [1 3] (a/calculate-ticket gw :p1 3))))

  (let [gw {:low-water-mark 1
            :high-water-mark 5}]
    (is (= [2 4] (a/calculate-ticket gw :p1 3))))

  (let [gw {:low-water-mark 1
            :high-water-mark 5}]
    (is (= [2 5] (a/calculate-ticket gw :p1 4))))

  (let [gw {:low-water-mark 1
            :high-water-mark 5}]
    (is (= [2 5] (a/calculate-ticket gw :p1 5))))

  (let [gw {:low-water-mark 1
            :high-water-mark 5}]
    (is (= [2 5] (a/calculate-ticket gw :p1 6))))

  (let [gw {:low-water-mark 4
            :high-water-mark 5}]
    (is (= [5 5] (a/calculate-ticket gw :p1 1))))

  (let [gw {:low-water-mark 5
            :high-water-mark 5}]
    (is (= -1 (a/calculate-ticket gw :p1 1))))

  (let [gw {:low-water-mark 4
            :high-water-mark 8
            :barriers {1 #{}}
            :barrier-index {1 6}}]
    (is (= [5 6] (a/calculate-ticket gw :p1 3))))

  (let [gw {:low-water-mark 4
            :high-water-mark 8
            :barriers {1 #{:p1} 2 #{}}
            :barrier-index {1 3
                            2 9}}]
    (is (= [5 7] (a/calculate-ticket gw :p1 3)))))

(deftest multi-peer-calculate-ticket
  (let [gw {:low-water-mark 4
            :high-water-mark 8
            :barriers {1 #{:p1} 2 #{}}
            :barrier-index {1 3
                            2 9}}]
    (is (= [3 3] (a/calculate-ticket gw :p2 3))))
  
  (let [gw {:low-water-mark 4
            :high-water-mark 8
            :barriers {1 #{:p1 :p2} 2 #{}}
            :barrier-index {1 3
                            2 9}}]
    (is (= [5 7] (a/calculate-ticket gw :p2 3))))
  
  (let [gw {:low-water-mark 8
            :high-water-mark 9
            :barriers {1 #{:p1 :p2} 2 #{}}
            :barrier-index {1 3
                            2 9}}]
    (is (= [9 9] (a/calculate-ticket gw :p2 3)))))
