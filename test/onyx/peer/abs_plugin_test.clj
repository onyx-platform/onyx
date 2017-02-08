(ns onyx.peer.abs-plugin-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.test-boilerplate :refer [build-job run-test-job]]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers!]]
            [onyx.api]))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest abs-plugin-test
  (let [n-messages 100
        task-opts {:onyx/batch-size 20}
        job (build-job [[:in :inc] [:inc :out]] 
                       [{:name :in
                         :type :seq 
                         :task-opts task-opts 
                         :input (map (fn [n] {:n n}) (range n-messages))}
                        {:name :inc
                         :type :fn 
                         :task-opts (assoc task-opts :onyx/fn ::my-inc)}
                        {:name :out
                         :type :async-out
                         :chan-size 10000000
                         :task-opts task-opts}]
                       :onyx.task-scheduler/balanced)
        output (run-test-job job 3)]
    (is (= (set (map (fn [x] {:n (inc x)}) (range n-messages)))
           (set (:out output))))))
