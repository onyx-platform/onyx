(ns onyx.scheduler.balanced-generative-test
  (:require [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.system]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [onyx.log.replica :as replica]
            [onyx.log.commands.common :as common]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [taoensso.timbre :refer [info]]))

(def onyx-id (java.util.UUID/randomUUID))

(def peer-config
  {:onyx/tenancy-id onyx-id
   :onyx.messaging/impl :dummy-messenger
   :onyx.peer/try-join-once? true})

(def base-replica 
  (merge replica/base-replica
         {:job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :dummy-messenger}}))

(def messenger (dummy-messenger {}))

(def job
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/min-peers 4
              :onyx/max-peers 4
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest combined-jobs
  (let [n-jobs 4
        n-peers 21
        job-ids (repeatedly n-jobs #(java.util.UUID/randomUUID))
        jobs-rets (map (fn [job-id]
                         (api/create-submit-job-entry job-id
                                                      peer-config
                                                      job
                                                      (planning/discover-tasks (:catalog job) (:workflow job))))
                       job-ids)]
    (checking
     "Checking peers are allocated to job 1 even though job 2 is submitted and can't start."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 n-peers))
                         :jobs {:queue jobs-rets})
         :log []
         :peer-choices []}))]
     (is (= 3 (count (:jobs replica))))
     (println "Jobs" (:jobs replica))
     (println "Allocatios" (:allocations replica))
     #_(let [[t1 t2 t3] (:tasks (:args job-1-rets))
           [t4 t5 t6] (:tasks (:args job-2-rets))]
       (is (= 3 (count (get (get (:allocations replica) job-1-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) job-1-id) t2))))
       (is (= 3 (count (get (get (:allocations replica) job-1-id) t3))))

       (is (= 0 (count (get (get (:allocations replica) job-2-id) t4))))
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t5))))
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t6)))))
     
     )))
