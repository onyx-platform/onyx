(ns onyx.scheduler.percentage-multi-job-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.generators :as log-gen]
            [onyx.test-helper :refer [job-allocation-counts get-counts]]
            [onyx.static.planning :as planning]
            [onyx.api]))

(def peer-config
  {:onyx/id "my-id"
   :onyx.messaging/impl :atom
   :onyx.peer/job-scheduler :onyx.job-scheduler/percentage})

(deftest log-percentage-multi-job
  (let [job-1-id "job-1"
        job-1 {:workflow [[:a :b]]
               :catalog [{:onyx/name :a
                          :onyx/plugin :onyx.test-helper/dummy-input
                          :onyx/type :input
                          :onyx/medium :dummy
                          :onyx/batch-size 20}

                         {:onyx/name :b
                          :onyx/plugin :onyx.test-helper/dummy-output
                          :onyx/type :output
                          :onyx/medium :dummy
                          :onyx/batch-size 20}]
               :percentage 70
               :task-scheduler :onyx.task-scheduler/balanced}
        job-2-id "job-2"
        job-2 {:workflow [[:c :d]]
               :catalog [{:onyx/name :c
                          :onyx/plugin :onyx.test-helper/dummy-input
                          :onyx/type :input
                          :onyx/medium :dummy
                          :onyx/batch-size 20}

                         {:onyx/name :d
                          :onyx/plugin :onyx.test-helper/dummy-output
                          :onyx/type :output
                          :onyx/medium :dummy
                          :onyx/batch-size 20}]
               :task-scheduler :onyx.task-scheduler/balanced
               :percentage 30}
        job-entry-1 (onyx.api/create-submit-job-entry
                     job-1-id
                     peer-config
                     job-1
                     (planning/discover-tasks (:catalog job-1) (:workflow job-1)))
        job-entry-2 (onyx.api/create-submit-job-entry
                     job-2-id
                     peer-config
                     job-2
                     (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]
    (checking
     "70/30% split for percentage job scheduler succeeded"
     (times 50)
     [{:keys [replica entries]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica {:job-scheduler :onyx.job-scheduler/percentage
                   :messaging {:onyx.messaging/impl :atom}}
         :message-id 0
         :entries
         (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 10))
                :job-1 {:queue [job-entry-1]}
                :job-2 {:queue [job-entry-2]})}))]
     (is (= (apply + (map count (vals (get-in replica [:allocations job-1-id])))) 7))
     (is (= (apply + (map count (vals (get-in replica [:allocations job-2-id])))) 3)))))

(deftest log-percentage-multi-job-rebalance
  (let [job-1-id "job-1"
        job-1 {:workflow [[:a :b]]
               :catalog [{:onyx/name :a
                          :onyx/plugin :onyx.test-helper/dummy-input
                          :onyx/type :input
                          :onyx/medium :dummy
                          :onyx/batch-size 20}

                         {:onyx/name :b
                          :onyx/plugin :onyx.test-helper/dummy-output
                          :onyx/type :output
                          :onyx/medium :dummy
                          :onyx/batch-size 20}]
               :percentage 70
               :task-scheduler :onyx.task-scheduler/balanced}
        job-2-id "job-2"
        job-2 {:workflow [[:c :d]]
               :catalog [{:onyx/name :c
                          :onyx/plugin :onyx.test-helper/dummy-input
                          :onyx/type :input
                          :onyx/medium :dummy
                          :onyx/batch-size 20}

                         {:onyx/name :d
                          :onyx/plugin :onyx.test-helper/dummy-output
                          :onyx/type :output
                          :onyx/medium :dummy
                          :onyx/batch-size 20}]
               :task-scheduler :onyx.task-scheduler/balanced
               :percentage 30}
        job-entry-1 (onyx.api/create-submit-job-entry
                     job-1-id
                     peer-config
                     job-1
                     (planning/discover-tasks (:catalog job-1) (:workflow job-1)))
        job-entry-2 (onyx.api/create-submit-job-entry
                     job-2-id
                     peer-config
                     job-2
                     (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]
    (checking
     "70/30% split for percentage job scheduler succeeded after rebalance"
     (times 50)
     [{:keys [replica entries]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica {:job-scheduler :onyx.job-scheduler/percentage
                   :messaging {:onyx.messaging/impl :atom}}
         :message-id 0
         :entries
         (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 2 10))
                :job-1 {:queue [job-entry-1]}
                :job-2 {:queue [job-entry-2]})}))]
     (is (= (apply + (map count (vals (get-in replica [:allocations job-1-id])))) 14))
     (is (= (apply + (map count (vals (get-in replica [:allocations job-2-id])))) 6)))))
