(ns onyx.coordinator.planning-test
  (:require [clojure.test :refer :all]
            [onyx.coordinator.planning :as planning]))

(deftest queue->fn->queue
  (let [catalog [{:onyx/name :in
                  :onyx/type :queue
                  :onyx/medium :hornetq
                  :hornetq/queue-name "in-queue"}
                 {:onyx/name :inc
                  :onyx/type :transformer
                  :onyx/fn :my.ns/inc
                  :onyx/consumation :sequential}
                 {:onyx/name :out
                  :onyx/type :queue
                  :onyx/medium :hornetq
                  :hornetq/queue-name "out-queue"}]
        workflow {:in {:inc :out}}]
    (is (= (planning/discover-tasks catalog workflow)
           #{{:name :in
              :phase 1
              :complete? false}
             {:name :inc
              :phase 2
              :complete? false
              :ingress-queue "in-queue"
              :egress-queue "out-queue"}
             {:name :out
              :phase 3
              :complete? false}}))))

(deftest queue->fn->fn->queue
  (let [catalog [{:onyx/name :in
                  :onyx/type :queue
                  :onyx/medium :hornetq
                  :hornetq/queue-name "in-queue"}
                 {:onyx/name :inc
                  :onyx/type :transformer
                  :onyx/fn :my.ns/inc
                  :onyx/consumation :sequential}
                 {:onyx-name :out
                  :onyx/type :queue
                  :onyx/medium :hornetq
                  :hornetq/queue-name "out-queue"}]
        workflow {:in {:inc {:inc :out}}}]
    (is (= (planning/discover-tasks catalog workflow)
           #{{:name :in
              :phase 1
              :complete? true}
             {:name :inc
              :phase 2
              :complete? false
              :ingress-queue "in-queue"
              :egress-queue "???"}
             {:name :inc
              :phase 3
              :complete? false
              :ingress-queue "???"
              :egress-queue "out-queue"}
             {:name :out
              :phase 4
              :complete? false}}))))

