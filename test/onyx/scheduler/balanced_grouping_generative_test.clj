(ns onyx.scheduler.balanced-grouping-generative-test
  (:require [onyx.log.generators :as log-gen]
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
            [onyx.messaging.protocols.messenger :as m]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.log.commands.common :as common]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [taoensso.timbre :refer [info]]))

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
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 4
              :onyx/max-peers 4
              :onyx/flux-policy :kill
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-2-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")

(def job-2
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 10
              :onyx/flux-policy :continue
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-3-id #uuid "5813d2ec-c486-4428-833d-e8373910ae14")

(def job-3
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 2
              :onyx/flux-policy :kill
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-4-id #uuid "60180f08-60b9-4584-9900-93dbbe2c3905")

(def job-4
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.test-helper/dummy-input
              :onyx/type :input
              :onyx/medium :dummy
              :onyx/batch-size 20}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 4
              :onyx/flux-policy :kill
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.test-helper/dummy-output
              :onyx/type :output
              :onyx/medium :dummy
              :onyx/batch-size 20}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest min-peers-one-job-upper-bound
  (let [rets (api/create-submit-job-entry
              job-1-id
              peer-config
              job-1
              (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]
    (checking
     "Checking that exactly 4 peers are assigned to task B."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 6))
                         :job-1 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       (is (= 1 (count (get (get (:allocations replica) job-1-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) job-1-id) t2))))
       (is (= 1 (count (get (get (:allocations replica) job-1-id) t3))))))))

(deftest min-peers-one-job-no-upper-bound
  (let [rets (api/create-submit-job-entry
              job-4-id
              peer-config
              job-4
              (planning/discover-tasks (:catalog job-4) (:workflow job-4)))]
    (checking
     "Checking at least 4 tasks are assigned to B."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 14))
                         :job-4 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       ;; If the job is submitted first, the second case occurs. Otherwise the first
       ;; case pins task B to 4 peers.
       (is
        (= true
           (or (and (= 5 (count (get (get (:allocations replica) job-4-id) t1)))
                    (= 4 (count (get (get (:allocations replica) job-4-id) t2)))
                    (= 5 (count (get (get (:allocations replica) job-4-id) t3))))
               (and (= 5 (count (get (get (:allocations replica) job-4-id) t1)))
                    (= 5 (count (get (get (:allocations replica) job-4-id) t2)))
                    (= 4 (count (get (get (:allocations replica) job-4-id) t3)))))))))))

(deftest min-peers-not-enough-peers
  (let [rets (api/create-submit-job-entry
              job-2-id
              peer-config
              job-2
              (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]
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
                         :job-2 {:queue [rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t1))))
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t2))))
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t3))))))))

(deftest combined-jobs
  (let [job-1-rets (api/create-submit-job-entry
                    job-1-id
                    peer-config
                    job-1
                    (planning/discover-tasks (:catalog job-1) (:workflow job-1)))
        job-2-rets (api/create-submit-job-entry
                    job-2-id
                    peer-config
                    job-2
                    (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]
    (checking
     "Checking peers are allocated to job 1 even though job 2 is submitted and can't start."
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica 
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 10))
                         :job-1 {:queue [job-1-rets]}
                         :job-2 {:queue [job-2-rets]})
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args job-1-rets))
           [t4 t5 t6] (:tasks (:args job-2-rets))]
       (is (= 3 (count (get (get (:allocations replica) job-1-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) job-1-id) t2))))
       (is (= 3 (count (get (get (:allocations replica) job-1-id) t3))))

       (is (= 0 (count (get (get (:allocations replica) job-2-id) t4))))
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t5))))
       (is (= 0 (count (get (get (:allocations replica) job-2-id) t6))))))))

(deftest scale-continue-flux-policy
  (let [continue-job-id "grouping-job-1"
        grouping-slot-job
        {:workflow [[:a :b] [:b :c]]
         :catalog [{:onyx/name :a
                    :onyx/plugin :onyx.test-helper/dummy-input
                    :onyx/type :input
                    :onyx/medium :dummy
                    :onyx/batch-size 20}

                   {:onyx/name :b
                    :onyx/fn :mock/fn
                    :onyx/type :function
                    :onyx/group-by-kw :mock-key
                    :onyx/flux-policy :continue
                    :onyx/batch-size 20}

                   {:onyx/name :c
                    :onyx/plugin :onyx.test-helper/dummy-output
                    :onyx/type :output
                    :onyx/medium :dummy
                    :onyx/batch-size 20}]
         :task-scheduler :onyx.task-scheduler/balanced}
        job-1-rets (api/create-submit-job-entry
                     continue-job-id
                     peer-config
                     grouping-slot-job
                     (planning/discover-tasks (:catalog grouping-slot-job) (:workflow grouping-slot-job)))]
    (checking
     "Checking grouping task recovers slots, and job1 continues running when peers leave, and job2 is killed"
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica
         :message-id 0
         :entries (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 16))
                      (assoc :job-1 {:queue [job-1-rets]})
                      (assoc :leave-1 {:predicate (fn [replica entry]
                                                    (some #{:g1-p1} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]})
                      (assoc :leave-2 {:predicate (fn [replica entry]
                                                    (some #{:g1-p2} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p2 :group-id :g1}}]})
                      (assoc :leave-3 {:predicate (fn [replica entry]
                                                    (some #{:g1-p3} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p3 :group-id :g1}}]})
                      (assoc :leave-4 {:predicate (fn [replica entry]
                                                    (some #{:g1-p4} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p4 :group-id :g1}}]}))
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args job-1-rets))]
       (is (= 12 (count (:peers replica))))
       (is (= 4 (count (get (get (:allocations replica) continue-job-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) continue-job-id) t2))))
       (is (= 4 (count (get (get (:allocations replica) continue-job-id) t3))))))))

(deftest recover-slots
  (let [grouping-slot-job-id "grouping-job-1"
        grouping-slot-job
        {:workflow [[:a :b] [:b :c]]
         :catalog [{:onyx/name :a
                    :onyx/plugin :onyx.test-helper/dummy-input
                    :onyx/type :input
                    :onyx/medium :dummy
                    :onyx/batch-size 20}

                   {:onyx/name :b
                    :onyx/fn :mock/fn
                    :onyx/type :function
                    :onyx/group-by-kw :mock-key
                    :onyx/min-peers 4
                    :onyx/max-peers 4
                    :onyx/flux-policy :recover
                    :onyx/batch-size 20}

                   {:onyx/name :c
                    :onyx/plugin :onyx.test-helper/dummy-output
                    :onyx/type :output
                    :onyx/medium :dummy
                    :onyx/batch-size 20}]
         :task-scheduler :onyx.task-scheduler/balanced}
        job-1-rets (api/create-submit-job-entry
                     grouping-slot-job-id
                     peer-config
                     grouping-slot-job
                     (planning/discover-tasks (:catalog grouping-slot-job) (:workflow grouping-slot-job)))
        job-2-id "grouping-job-2"
        job-2-rets (api/create-submit-job-entry
                     job-2-id
                     peer-config
                     grouping-slot-job
                     (planning/discover-tasks (:catalog grouping-slot-job) (:workflow grouping-slot-job)))]
    (checking
     "Checking grouping task recovers slots, and job1 continues running when peers leave, and job2 is killed"
     (times 50)
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica base-replica
         :message-id 0
         :entries (-> (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 12))
                      (assoc :job-1 {:queue [job-1-rets]})
                      (assoc :job-2 {:queue [job-2-rets {:fn :kill-job :args {:job job-2-id}}]})
                      (assoc :leave-1 {:predicate (fn [replica entry]
                                                    (some #{:g1-p1} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p1 :group-id :g1}}]})
                      (assoc :leave-2 {:predicate (fn [replica entry]
                                                    (some #{:g1-p2} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p2 :group-id :g1}}]})
                      (assoc :leave-3 {:predicate (fn [replica entry]
                                                    (some #{:g1-p3} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p3 :group-id :g1}}]})
                      (assoc :leave-4 {:predicate (fn [replica entry]
                                                    (some #{:g1-p4} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p4 :group-id :g1}}]})
                      (assoc :leave-5 {:predicate (fn [replica entry]
                                                    (some #{:g1-p5} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p5 :group-id :g1}}]})
                      (assoc :leave-6 {:predicate (fn [replica entry]
                                                    (some #{:g1-p6} (:peers replica)))
                                       :queue [{:fn :leave-cluster :args {:id :g1-p6 :group-id :g1}}]}))
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args job-1-rets))]
       (is (= #{0 1 2 3} (set (vals (get-in replica [:task-slot-ids grouping-slot-job-id t2])))))
       (is (= 6 (count (:peers replica))))
       (is (= 1 (count (get (get (:allocations replica) grouping-slot-job-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) grouping-slot-job-id) t2))))
       (is (= 1 (count (get (get (:allocations replica) grouping-slot-job-id) t3))))))))
