(ns onyx.scheduler.scheduler-test
  (:require [clojure.test :refer :all]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.api]))

(deftest jitter
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

#_(run-tests)
