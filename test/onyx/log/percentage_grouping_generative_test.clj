(ns onyx.log.percentage-grouping-generative-test
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
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def onyx-id (java.util.UUID/randomUUID))

(def peer-config 
  {:onyx/id onyx-id
   :onyx.messaging/impl :dummy-messenger
   :onyx.peer/try-join-once? true})

(def messenger (dummy-messenger {}))

(def job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/ident :core.async/read-from-chan
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/percentage 20
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

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
              :onyx/ident :core.async/write-to-chan
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/percentage 50
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/percentage})

(def job-2-id #uuid "60180f08-60b9-4584-9900-93dbbe2c3905")

(def job-2
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/ident :core.async/read-from-chan
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/percentage 20
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 4
              :onyx/flux-policy :kill
              :onyx/percentage 60
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/ident :core.async/write-to-chan
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/percentage 20
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
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
        {:replica {:job-scheduler :onyx.job-scheduler/greedy
                   :messaging {:onyx.messaging/impl :dummy-messenger}}
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 10)) :job-1 [rets])
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
        {:replica {:job-scheduler :onyx.job-scheduler/greedy
                   :messaging {:onyx.messaging/impl :dummy-messenger}}
         :message-id 0
         :entries (assoc (log-gen/generate-join-queues (log-gen/generate-peer-ids 10)) :job-2 [rets])
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))
           c1 (count (get (get (:allocations replica) job-2-id) t1))
           c2 (count (get (get (:allocations replica) job-2-id) t2))
           c3 (count (get (get (:allocations replica) job-2-id) t3))]
       (is (>= c2 4))
       (is (= 10 (+ c1 c2 c3)))))))
