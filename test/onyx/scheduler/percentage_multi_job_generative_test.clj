(ns onyx.scheduler.percentage-multi-job-generative-test
  (:require [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts get-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(deftest percentage-multi-job-test
  (let [percentages-peer-config {:onyx/tenancy-id #uuid "9fd09779-749b-4668-b373-bdf3eeb98a8f"
                                 :onyx.messaging/impl :aeron
                                 :onyx.peer/job-scheduler :onyx.job-scheduler/percentage}
        job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608"
        job-1 {:workflow [[:a :b]]
               :percentage 70
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
               :task-scheduler :onyx.task-scheduler/balanced}

        job-2-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14"
        job-2 {:workflow [[:c :d]]
               :percentage 30
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
               :task-scheduler :onyx.task-scheduler/balanced}] 
    (checking
      "Checking percentage multi job test, replicates onyx.scheduling.percentage-multi-job-test"
      (times 50)
      [{:keys [replica log peer-choices]}
       (log-gen/apply-entries-gen
         (gen/return
           {:replica {:job-scheduler :onyx.job-scheduler/percentage
                      :messaging {:onyx.messaging/impl :aeron}}
            :message-id 0
            :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 24))
                            :leave-1 {:predicate (fn [replica entry]
                                                   (some #{:g1-p1} (:peers replica)))
                                      :queue [{:fn :leave-cluster
                                               :args {:id :g1-p1
                                                      :group-id :g1}}]}
                            :leave-2 {:predicate (fn [replica entry]
                                                   (some #{:g1-p2} (:peers replica)))
                                      :queue [{:fn :leave-cluster
                                               :args {:id :g1-p2
                                                      :group-id :g1}}]}
                            :leave-3 {:predicate (fn [replica entry]
                                                   (some #{:g1-p3} (:peers replica)))
                                      :queue [{:fn :leave-cluster
                                               :args {:id :g1-p3
                                                      :group-id :g1}}]}
                            :leave-4 {:predicate (fn [replica entry]
                                                   (some #{:g1-p4} (:peers replica)))
                                      :queue [{:fn :leave-cluster
                                               :args {:id :g1-p4
                                                      :group-id :g1}}]}
                            :job-1 {:queue [(api/create-submit-job-entry
                                              job-1-id
                                              percentages-peer-config
                                              job-1
                                              (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
                            :job-2 {:queue [(api/create-submit-job-entry
                                              job-2-id
                                              percentages-peer-config
                                              job-2
                                              (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]})
            :log []
            :peer-choices []}))]
      (is (= [14 6]
             (map (partial apply +)
                  (map vals (get-counts replica
                                        [{:job-id job-1-id}
                                         {:job-id job-2-id}]))))))))
