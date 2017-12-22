(ns onyx.scheduler.percentage-grouping-generative-test
  (:require [clojure.test :refer :all]
            [clojure.set :refer [intersection]]
            [onyx.messaging.protocols.messenger :as m]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.log.generators :as log-gen]
            [onyx.log.replica :as replica]
            [onyx.test-helper :refer [job-allocation-counts]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.planning :as planning]
            [onyx.api :as api]))

(def onyx-id (random-uuid))

(def peer-config
  {:onyx/tenancy-id onyx-id
   :onyx.messaging/impl :aeron
   :onyx.peer/try-join-once? true})

(def base-replica 
  (merge replica/base-replica
         {:job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))

(def messenger-group (m/build-messenger-group peer-config))
(def messenger (m/build-messenger peer-config messenger-group {} nil nil))

(def job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/percentage 20
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 4
              :onyx/max-peers 4
              :onyx/flux-policy :kill
              :onyx/percentage 30
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/percentage 50
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/percentage})

(def job-2-id #uuid "60180f08-60b9-4584-9900-93dbbe2c3905")

(def job-2
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/percentage 20
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 4
              :onyx/flux-policy :kill
              :onyx/percentage 60
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/percentage 20
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/percentage})

(def job-3-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")

(def job-3
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/percentage 20
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 10
              :onyx/percentage 50
              :onyx/flux-policy :continue
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/percentage 30
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/percentage})

(def job-4-id #uuid "c4774cac-57d0-4993-b19e-3d1de20e61ca")

(def job-4
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/percentage 25
              :onyx/max-peers 1
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 3
              :onyx/percentage 50
              :onyx/flux-policy :continue
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/percentage 25
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/percentage})

(deftest min-peers-one-job-upper-bound
  (let [rets (api/create-submit-job-entry
              job-1-id
              peer-config
              job-1
              (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]
    (checking
     "Checking that exactly 4 peers are assigned to task B, and that the extra 10%
      needed to get 4 peers for B are taken from task C."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 10))
                         :job-1 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       (is (= 2 (count (get (get (:allocations replica) job-1-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) job-1-id) t2))))
       (is (= 4 (count (get (get (:allocations replica) job-1-id) t3))))))))

(deftest min-peers-one-job-no-upper-bound
  (let [rets (api/create-submit-job-entry
              job-2-id
              peer-config
              job-2
              (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]
    (checking
     "Checking that 6 tasks are assigned to B."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 10))
                         :job-2 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))
           c1 (count (get (get (:allocations replica) job-2-id) t1))
           c2 (count (get (get (:allocations replica) job-2-id) t2))
           c3 (count (get (get (:allocations replica) job-2-id) t3))]
       (is (>= c2 4))
       (is (= 10 (+ c1 c2 c3)))))))

(deftest min-peers-not-enough-peers
  (let [rets (api/create-submit-job-entry
              job-3-id
              peer-config
              job-3
              (planning/discover-tasks (:catalog job-3) (:workflow job-3)))]
    (checking
     "Checking no peers are ever allocated to this job since this job needs at least
      12 peers to run."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 6))
                         :job-3 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       (is (= 0 (count (get (get (:allocations replica) job-3-id) t1))))
       (is (= 0 (count (get (get (:allocations replica) job-3-id) t2))))
       (is (= 0 (count (get (get (:allocations replica) job-3-id) t3))))))))

(deftest min-and-max-peers-compose
  (let [rets (api/create-submit-job-entry
              job-4-id
              peer-config
              job-4
              (planning/discover-tasks (:catalog job-4) (:workflow job-4)))]
    (checking
     "Checking that using min and max peers in the same catalog works together"
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 10))
                         :job-4 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       (is (= 1 (count (get (get (:allocations replica) job-4-id) t1))))
       (is (<= 3 (count (get (get (:allocations replica) job-4-id) t2))))
       (is (>= 6 (count (get (get (:allocations replica) job-4-id) t3))))
       (is (= 9 (+ (count (get (get (:allocations replica) job-4-id) t2))
                   (count (get (get (:allocations replica) job-4-id) t3)))))))))
