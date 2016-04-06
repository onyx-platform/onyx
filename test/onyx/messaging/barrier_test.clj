(ns onyx.messaging.barrier-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.messaging.aeron :as a]
            [onyx.types :refer [map->Barrier]]
            [onyx.api])
  (:import [io.aeron.logbuffer ControlledFragmentHandler ControlledFragmentHandler$Action]))

(deftest test-rotate
  (is (= [] (a/rotate [])))
  (is (= [2 3 1] (a/rotate [1 2 3])))
  (is (= [3 1 2] (a/rotate [2 3 1])))
  (is (= [1 2 3] (a/rotate [3 1 2]))))

(comment (deftest aeron-ticketing-response-test
  (testing "local index equal to ticket"
    (let [results (atom [])
          local-counter (atom {})
          shared-counter (atom {:t1 {:p1 nil}})
          current-ticket (atom nil)
          n-desired 3
          m {:type :segment :dst-task-id :t1 :src-peer-id :p1}
          f (partial a/handle-deserialized-message n-desired results local-counter shared-counter current-ticket)]
      (is (= ControlledFragmentHandler$Action/CONTINUE (f :t1 :p1 m)))
      (is (= {:t1 {:p1 0}} @local-counter))
      (is (= {:t1 {:p1 0}} @shared-counter))
      (is (= [m] @results))
      (is (nil? @current-ticket))))

  (testing "local index behind ticket taken"
    (let [results (atom [])
          local-counter (atom {:t1 {:p1 0}})
          shared-counter (atom {:t1 {:p1 2}})
          current-ticket (atom nil)
          n-desired 3
          m {:type :segment :dst-task-id :t1 :src-peer-id :p1}
          f (partial a/handle-deserialized-message n-desired results local-counter shared-counter current-ticket)]
      (is (= ControlledFragmentHandler$Action/CONTINUE (f :t1 :p1 m)))
      (is (= {:t1 {:p1 1}} @local-counter))
      (is (= {:t1 {:p1 3}} @shared-counter))
      (is (= 3 @current-ticket))
      (is (= [] @results))))

  (testing "segment for another task"
    (let [results (atom [])
          local-counter (atom {})
          shared-counter (atom {})
          current-ticket (atom nil)
          n-desired 3
          m {:type :segment :dst-task-id :t2 :src-peer-id :p1}
          f (partial a/handle-deserialized-message n-desired results local-counter shared-counter current-ticket)]
      (is (= ControlledFragmentHandler$Action/CONTINUE (f :t1 :p1 m)))
      (is (= {} @local-counter))
      (is (= {} @shared-counter))
      (is (= [] @results))
      (is (nil? @current-ticket))))

  (testing "segment from another peer id"
    (let [results (atom [])
          local-counter (atom {})
          shared-counter (atom {})
          current-ticket (atom nil)
          n-desired 3
          m {:type :segment :dst-task-id :t1 :src-peer-id :p2}
          f (partial a/handle-deserialized-message n-desired results local-counter shared-counter current-ticket)]
      (is (= ControlledFragmentHandler$Action/CONTINUE (f :t1 :p1 m)))
      (is (= {} @local-counter))
      (is (= {} @shared-counter))
      (is (= [] @results))
      (is (nil? @current-ticket))))

  (testing "result state full"
    (let [results (atom [1 2 3])
          local-counter (atom {})
          shared-counter (atom {})
          current-ticket (atom nil)
          n-desired 3
          m {:type :segment :dst-task-id :t1 :src-peer-id :p1}
          f (partial a/handle-deserialized-message n-desired results local-counter shared-counter current-ticket)]
      (is (= ControlledFragmentHandler$Action/ABORT (f :t1 :p1 m)))
      (is (= {} @local-counter))
      (is (= {} @shared-counter))
      (is (= [1 2 3] @results))
      (is (nil? @current-ticket))))

  (testing "local index behind, encounters barrier"
    (let [results (atom [])
          local-counter (atom {:t1 {:p1 0}})
          shared-counter (atom {:t1 {:p1 2}})
          current-ticket (atom nil)
          n-desired 3
          m (map->Barrier {:src-peer-id :p1 :dst-task-id :t1 })
          f (partial a/handle-deserialized-message n-desired results local-counter shared-counter current-ticket)]
      (is (= ControlledFragmentHandler$Action/BREAK (f :t1 :p1 m)))
      (is (= {:t1 {:p1 1}} @local-counter))
      (is (= {:t1 {:p1 3}} @shared-counter))
      (is (= 3 @current-ticket))
      (is (= [m] @results))))))
