(ns onyx.scheduler.tagged-constraints-test
  (:require [clojure.test :refer :all]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
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
