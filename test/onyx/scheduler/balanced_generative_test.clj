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
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test :refer :all]
            [onyx.log.replica :as replica]
            [onyx.log.commands.common :as common]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking for-all]]
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

(defspec allocate-as-many-jobs-as-possible {:num-tests (times 10)}
  (let [;; TODO, make these parameters part of the property testing
        min-peers-per-job 6]
    (for-all [n-jobs (gen/such-that #(< % 10) gen/s-pos-int 1000)
              n-jobs-to-allocate (gen/elements (range 0 (inc n-jobs))) 
              ;; add excess peers, but not enough to allocate an extra job
              n-excess (gen/elements (range 1 6))
              n-peers (gen/return (+ n-excess (* min-peers-per-job n-jobs-to-allocate)))
              job-ids (gen/vector gen/uuid n-jobs)
              jobs-rets (gen/return (mapv (fn [job-id]
                                            (api/create-submit-job-entry job-id
                                                                         peer-config
                                                                         job
                                                                         (planning/discover-tasks (:catalog job) 
                                                                                                  (:workflow job))))
                                          job-ids))
              {:keys [replica log peer-choices]} 
              (log-gen/apply-entries-gen
               (gen/return
                {:replica base-replica 
                 :message-id 0
                 :entries (assoc (log-gen/generate-join-queues (log-gen/generate-group-and-peer-ids 1 n-peers))
                                 :jobs {:queue jobs-rets})
                 :log []
                 :peer-choices []}))]
             (is (= n-jobs-to-allocate 
                    (count (:allocations replica))) "as many jobs are allocated as possible")
             (let [allocation-counts (map (fn [job-id]
                                            (reduce + (map (comp count val) 
                                                           (get-in replica [:allocations job-id]))))
                                          job-ids)]
               (let [sorted-cnts (-> allocation-counts
                                     set
                                     (disj 0)
                                     vec
                                     sort)]
                 (is (or (< (count sorted-cnts) 2)
                         ;; at max two possible counts can exist because everything should be balanced
                         ;; e.g. 7 on one job, and 8 on two jobs
                         (and (= (count sorted-cnts) 2)
                              (= (inc (first sorted-cnts)) (second sorted-cnts))))))
               (is (every? (fn [cnt]
                             (or (zero? cnt) (>= cnt 6)))
                           allocation-counts)
                   ["no partially allocated jobs" (:allocations replica)])))))
