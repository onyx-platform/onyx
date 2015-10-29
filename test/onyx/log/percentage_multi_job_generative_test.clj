(ns onyx.log.percentage-multi-job-generative-test
  (:require [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts get-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [onyx.log.replica-invariants :refer [standard-invariants]]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(deftest percentage-multi-job-test
  (let [percentages-peer-config {:onyx/id #uuid "9fd09779-749b-4668-b373-bdf3eeb98a8f"
                                 :onyx.messaging/impl :dummy-messenger
                                 :onyx.peer/job-scheduler :onyx.job-scheduler/percentage}
        job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608"
        job-1 {:workflow [[:a :b]]
               :percentage 70
               :catalog [{:onyx/name :a
                          :onyx/plugin :onyx.plugin.core-async/input
                          :onyx/type :input
                          :onyx/medium :core.async
                          :onyx/batch-size 20
                          :onyx/doc "Reads segments from a core.async channel"}

                         {:onyx/name :b
                          :onyx/plugin :onyx.plugin.core-async/output
                          :onyx/type :output
                          :onyx/medium :core.async
                          :onyx/batch-size 20
                          :onyx/doc "Writes segments to a core.async channel"}]
               :task-scheduler :onyx.task-scheduler/balanced}

        job-2-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14"
        job-2 {:workflow [[:c :d]]
               :percentage 30
               :catalog [{:onyx/name :c
                          :onyx/plugin :onyx.plugin.core-async/input
                          :onyx/type :input
                          :onyx/medium :core.async
                          :onyx/batch-size 20
                          :onyx/doc "Reads segments from a core.async channel"}

                         {:onyx/name :d
                          :onyx/plugin :onyx.plugin.core-async/output
                          :onyx/type :output
                          :onyx/medium :core.async
                          :onyx/batch-size 20
                          :onyx/doc "Writes segments to a core.async channel"}]
               :task-scheduler :onyx.task-scheduler/balanced}] 
    (checking
      "Checking percentage multi job test, replicates onyx.log.percentage-multi-job-test"
      (times 50)
      [{:keys [replica log peer-choices]}
       (log-gen/apply-entries-gen
         (gen/return
           {:replica {:job-scheduler :onyx.job-scheduler/percentage
                      :messaging {:onyx.messaging/impl :dummy-messenger}}
            :message-id 0
            :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 20))
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
      (standard-invariants replica)
      (is (= #{:active} (set (vals (:peer-state replica)))))
      (is (= [14 6] 
             (map (partial apply +)
                  (get-counts replica [{:job-id job-1-id} {:job-id job-2-id}])))))))
