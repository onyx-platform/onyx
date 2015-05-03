(ns onyx.log.grouping-generative-task
  (:require [onyx.messaging.dummy-messenger :refer [->DummyMessenger]]
            [onyx.log.generators :as log-gen]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [onyx.static.planning :as planning]
            [onyx.test-helper :refer [job-allocation-counts]]
            [clojure.set :refer [intersection]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]))

(def onyx-id (java.util.UUID/randomUUID))

(def peer-config 
  {:onyx/id onyx-id
   :onyx.messaging/impl :dummy-messenger})

(def messenger (->DummyMessenger))

(def job-1-id #uuid "f55c14f0-a847-42eb-81bb-0c0390a88608")

(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/ident :core.async/read-from-chan
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/group-by-kw :mock-key
              :onyx/min-peers 4
              :onyx/max-peers 4
              :onyx/flux-policy :kill
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/ident :core.async/write-to-chan
              :onyx/type :output
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(deftest min-peers-flux-kill
  (let [rets (api/create-submit-job-entry
              job-1-id
              peer-config 
              job-1 
              (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]
    (checking
     "Checking that exactly 4 peers are assigned to task B, and if that drops
      below 4, the job is killed due to the flux policy."
     1000
     [{:keys [replica log peer-choices]}
      (log-gen/apply-entries-gen
       (gen/return
        {:replica {:job-scheduler :onyx.job-scheduler/greedy
                   :messaging {:onyx.messaging/impl :dummy-messenger}}
         :message-id 0
         :entries (assoc (log-gen/generate-join-entries (log-gen/generate-peer-ids 6)) :job-1 [rets])
         :log []
         :peer-choices []}))]
     (let [[t1 t2 t3] (:tasks (:args rets))]
       (is (= 1 (count (get (get (:allocations replica) job-1-id) t1))))
       (is (= 4 (count (get (get (:allocations replica) job-1-id) t2))))
       (is (= 1 (count (get (get (:allocations replica) job-1-id) t3))))))))
