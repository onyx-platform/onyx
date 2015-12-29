(ns onyx.scheduler.scheduler-test
  (:require [clojure.test :refer :all]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.api]))

(deftest jitter-on-no-change
  (let [initial-allocations
        {:j1 {:t1 [:p2 :p1]
              :t2 [:p4 :p3]
              :t3 [:p5]}
         :j2 {:t4 [:p8 :p7 :p6]
              :t5 [:p9 :p10]}}]
    (is
     (= initial-allocations
        (:allocations
         (reconfigure-cluster-workload
          {:jobs [:j1 :j2]
           :allocations initial-allocations
           :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10]
           :tasks {:j1 [:t1 :t2 :t3]
                   :j2 [:t4 :t5]}
           :saturation {:j1 5 :j2 5}
           :task-schedulers {:j1 :onyx.task-scheduler/balanced
                             :j2 :onyx.task-scheduler/balanced}
           :job-scheduler :onyx.job-scheduler/balanced
           :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest jitter-on-add-peer
  (is
   (= {:j1 {:t1 [:p4 :p5] :t2 [:p2] :t3 [:p7]}
       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1 :j2]
         :allocations {:j1 {:t1 [:p4] :t2 [:p2] :t3 [:p7]}
                       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p1]}}
         :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7]
         :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest underwhelm-peers
  (is
   (= {:j1 {:t1 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {:j1 {:t1 []}}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1]}
         :task-saturation {:j1 {:t1 1}}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest not-enough-peers
  (is
   (= {}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {:j1 {:t1 []}}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1]}
         :min-required-peers {:j1 {:t1 10}}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest only-one-job-allocated
  (is
   (= {:j1 {:t1 [:p1 :p2 :p3]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1 :j2]
         :allocations {:j1 {:t1 []} :j2 {:t2 []}}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1] :j2 [:t2]}
         :min-required-peers {:j1 {:t1 3} :j2 {:t2 3}}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest even-distribution
  (is
   (= {:j1 {:t1 [:p6] :t2 [:p2] :t3 [:p1]}
       :j2 {:t4 [:p5] :t5 [:p3] :t6 [:p4]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1 :j2]
         :allocations {:j1 {:t1 []} :j2 {:t2 []}}
         :peers [:p1 :p2 :p3 :p4 :p5 :p6]
         :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest prefer-earlier-job
  (is
   (= {:j1 {:t1 [:p6 :p7] :t2 [:p2] :t3 [:p1]}
       :j2 {:t4 [:p5] :t5 [:p3] :t6 [:p4]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1 :j2]
         :allocations {:j1 {:t1 []} :j2 {:t2 []}}
         :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7]
         :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest skip-overloaded-jobs
  (is
   (= {:j1 {:t1 [:p4 :p5 :p6]}
       :j3 {:t3 [:p1 :p2 :p3]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1 :j2 :j3]
         :peers [:p1 :p2 :p3 :p4 :p5 :p6]
         :tasks {:j1 [:t1] :j2 [:t2] :j3 [:t3]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced
                           :j2 :onyx.task-scheduler/balanced
                           :j3 :onyx.task-scheduler/balanced}
         :min-required-peers {:j2 {:t2 100}}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))
