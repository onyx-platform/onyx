(ns onyx.scheduler.tagged-constraints-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [com.gfredericks.test.chuck :refer [times]]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.generators :as log-gen]
            [onyx.log.replica-invariants :refer [standard-invariants]]
            [onyx.static.planning :as planning]
            [onyx.api]))

(deftest no-peers-are-allocated-missing-tags
  (is
   (= {}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1 :t2 :t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :required-tags {:j1 {:t1 [:datomic]
                              :t2 [:datomic]
                              :t3 [:datomic]}}
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest peers-allocated-with-tags
  (is
   (= {:j1 {:t1 [:p3]
            :t2 [:p2]
            :t3 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1 :t2 :t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :required-tags {:j1 {:t1 [:datomic]
                              :t2 [:datomic]
                              :t3 [:datomic]}}
         :peer-tags {:p1 [:datomic] :p2 [:datomic] :p3 [:datomic]}
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest only-tagged-peers-allocated
  (is
   (= {:j1 {:t1 [:p4]
            :t2 [:p2]
            :t3 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {}
         :peers [:p1 :p2 :p3 :p4]
         :tasks {:j1 [:t1 :t2 :t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :required-tags {:j1 {:t1 [:datomic]
                              :t2 [:datomic]
                              :t3 [:datomic]}}
         :peer-tags {:p1 [:datomic]
                     :p2 [:datomic]
                     :p3 []
                     :p4 [:datomic]}
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest one-task-tagged
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p3]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1 :t2 :t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :required-tags {:j1 {:t1 [:datomic]}}
         :peer-tags {:p1 [:datomic]}
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest one-task-tagged-max-peers
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p3 :p4]
            :t3 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {}
         :peers [:p1 :p2 :p3 :p4]
         :tasks {:j1 [:t1 :t2 :t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :required-tags {:j1 {:t1 [:datomic]}}
         :peer-tags {:p1 [:datomic]}
         :task-saturation {:j1 {:t1 1}}
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest two-tags
  (is
   (= {:j1 {:t1 [:p1]
            :t2 [:p2]
            :t3 [:p3]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1 :t2 :t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :required-tags {:j1 {:t1 []
                              :t2 [:mysql :datomic]
                              :t3 [:datomic]}}
         :peer-tags {:p1 []
                     :p2 [:datomic :mysql]
                     :p3 [:datomic]}
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest two-jobs
  (is
   (= {:j1 {:t1 [:p7]
            :t2 [:p3]
            :t3 [:p5]}
       :j2 {:t4 [:p8]
            :t5 [:p4]
            :t6 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1 :j2]
         :allocations {:j1 {:t1 [:p7]
                            :t2 [:p3 :p4 :p5]
                            :t3 [:p8]}}
         :peers [:p1 :p3 :p4 :p5 :p7 :p8]
         :tasks {:j1 [:t1 :t2 :t3]
                 :j2 [:t4 :t5 :t6]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :task-saturation {:j1 {:t1 1 :t2 42 :t3 1}
                           :t2 {:t4 1 :t5 42 :t6 1}}
         :required-tags {:j1 {:t1 [:datomic]
                              :t2 []
                              :t3 []}
                         :j2 {:t4 [:datomic]
                              :t5 []
                              :t6 []}}
         :peer-tags {:p7 [:datomic]
                     :p8 [:datomic]}
         :messaging {:onyx.messaging/impl :aeron}})))))

(def onyx-id "tagged-gen-test-id")

(def job-1-id "job-1")

(def job-1
  {:workflow [[:a :b] [:b :c]]
   :catalog [{:onyx/name :a
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/max-peers 1
              :onyx/required-tags [:special-peer]
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :b
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :c
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/max-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def job-2-id "job-2")

(def job-2
  {:workflow [[:d :e] [:e :f]]
   :catalog [{:onyx/name :d
              :onyx/plugin :onyx.plugin.core-async/input
              :onyx/type :input
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/max-peers 1
              :onyx/doc "Reads segments from a core.async channel"}

             {:onyx/name :e
              :onyx/fn :mock/fn
              :onyx/type :function
              :onyx/batch-size 20}

             {:onyx/name :f
              :onyx/plugin :onyx.plugin.core-async/output
              :onyx/type :output
              :onyx/max-peers 1
              :onyx/medium :core.async
              :onyx/batch-size 20
              :onyx/required-tags [:special-peer]
              :onyx/doc "Writes segments to a core.async channel"}]
   :task-scheduler :onyx.task-scheduler/balanced})

(def peer-config
  {:onyx/id onyx-id
   :onyx.messaging/impl :dummy-messenger})

(deftest peer-leave-tagged
  (checking
    "Peers leaving keep the job running"
    (times 50)
    [{:keys [replica log entries peer-choices]}
     (log-gen/apply-entries-gen
      (gen/return
       {:replica {:job-scheduler :onyx.job-scheduler/balanced
                  :messaging {:onyx.messaging/impl :dummy-messenger}}
        :message-id 0
        :entries
        (assoc (merge (log-gen/generate-join-queues (log-gen/generate-peer-ids 6))
                      (log-gen/generate-join-queues (log-gen/generate-peer-ids 7 2) {:tags [:special-peer]}))
               :job-1 {:queue [(onyx.api/create-submit-job-entry
                                job-1-id
                                peer-config
                                job-1
                                (planning/discover-tasks (:catalog job-1) (:workflow job-1)))]}
               :job-2 {:queue [(onyx.api/create-submit-job-entry
                                job-2-id
                                peer-config
                                job-2
                                (planning/discover-tasks (:catalog job-2) (:workflow job-2)))]}
               :leave-1 {:queue [{:fn :leave-cluster :args {:id :p1}}]}
               :leave-2 {:queue [{:fn :leave-cluster :args {:id :p2}}]})
        :log []
        :peer-choices []}))]
    (standard-invariants replica)
    (let [task-a-id (first (get-in replica [:tasks job-1-id]))
          task-f-id (last (get-in replica [:tasks job-2-id]))
          task-a-peers (get-in replica [:allocations job-1-id task-a-id])
          task-f-peers (get-in replica [:allocations job-2-id task-f-id])]
      (is (some (into #{} task-a-peers) #{:p7 :p8}))
      (is (some (into #{} task-f-peers) #{:p7 :p8})))))
