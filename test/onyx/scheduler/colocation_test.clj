(ns onyx.scheduler.colocation-task
  (:require [clojure.test :refer :all]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.api]))

(deftest colocate-tasks-on-a-single-machine
  (is
   (=
    {:j1 {:t1 [:p4] :t2 [:p1] :t3 [:p2]}}
    (:allocations
     (reconfigure-cluster-workload
      {:messaging {:onyx.messaging/impl :aeron}
       :job-scheduler :onyx.job-scheduler/greedy
       :task-schedulers {:j1 :onyx.task-scheduler/colocated}
       :peers [:p1 :p2 :p3 :p4 :p5]
       :jobs [:j1]
       :tasks {:j1 [:t1 :t2 :t3]}
       :peer-sites {:p1 {:aeron/external-addr :a}
                    :p2 {:aeron/external-addr :a}
                    :p3 {:aeron/external-addr :b}
                    :p4 {:aeron/external-addr :a}
                    :p5 {:aeron/external-addr :b}}})))))

(deftest refuse-to-run-job-if-machine-not-big-enough
  (is
   (= {}
      (:allocations
       (reconfigure-cluster-workload
        {:messaging {:onyx.messaging/impl :aeron}
         :job-scheduler :onyx.job-scheduler/greedy
         :task-schedulers {:j1 :onyx.task-scheduler/colocated}
         :peers [:p1 :p2 :p3 :p4 :p5]
         :jobs [:j1]
         :tasks {:j1 [:t1 :t2 :t3]}
         :peer-sites {:p1 {:aeron/external-addr :a}
                      :p2 {:aeron/external-addr :a}
                      :p3 {:aeron/external-addr :b}
                      :p4 {:aeron/external-addr :c}
                      :p5 {:aeron/external-addr :b}}})))))

(deftest colocate-on-two-machines
  (is
   (=
    {:j1 {:t1 [:p3 :p5] :t2 [:p1 :p6] :t3 [:p2 :p4]}}
    (:allocations
     (reconfigure-cluster-workload
      {:messaging {:onyx.messaging/impl :aeron}
       :job-scheduler :onyx.job-scheduler/greedy
       :task-schedulers {:j1 :onyx.task-scheduler/colocated}
       :peers [:p1 :p2 :p3 :p4 :p5 :p6]
       :jobs [:j1]
       :tasks {:j1 [:t1 :t2 :t3]}
       :peer-sites {:p1 {:aeron/external-addr :a}
                    :p2 {:aeron/external-addr :a}
                    :p3 {:aeron/external-addr :a}
                    :p4 {:aeron/external-addr :b}
                    :p5 {:aeron/external-addr :b}
                    :p6 {:aeron/external-addr :b}}})))))

(deftest ban-small-machines
  (is
   (=
    {:j1 {:t1 [:p3 :p5] :t2 [:p1 :p6] :t3 [:p2 :p4]}}
    (:allocations
     (reconfigure-cluster-workload
      {:messaging {:onyx.messaging/impl :aeron}
       :job-scheduler :onyx.job-scheduler/greedy
       :task-schedulers {:j1 :onyx.task-scheduler/colocated}
       :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8]
       :jobs [:j1]
       :tasks {:j1 [:t1 :t2 :t3]}
       :peer-sites {:p1 {:aeron/external-addr :a}
                    :p2 {:aeron/external-addr :a}
                    :p3 {:aeron/external-addr :a}
                    :p4 {:aeron/external-addr :b}
                    :p5 {:aeron/external-addr :b}
                    :p6 {:aeron/external-addr :b}
                    :p7 {:aeron/external-addr :c}
                    :p8 {:aeron/external-addr :c}}})))))

(deftest colocate-on-three-machines
  (is
   (=
    {:j1 {:t1 [:p3 :p5 :p9]
          :t2 [:p1 :p6 :p8]
          :t3 [:p2 :p4 :p7]}}
    (:allocations
     (reconfigure-cluster-workload
      {:messaging {:onyx.messaging/impl :aeron}
       :job-scheduler :onyx.job-scheduler/greedy
       :task-schedulers {:j1 :onyx.task-scheduler/colocated}
       :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9]
       :jobs [:j1]
       :tasks {:j1 [:t1 :t2 :t3]}
       :peer-sites {:p1 {:aeron/external-addr :a}
                    :p2 {:aeron/external-addr :a}
                    :p3 {:aeron/external-addr :a}
                    :p4 {:aeron/external-addr :b}
                    :p5 {:aeron/external-addr :b}
                    :p6 {:aeron/external-addr :b}
                    :p7 {:aeron/external-addr :c}
                    :p8 {:aeron/external-addr :c}
                    :p9 {:aeron/external-addr :c}}})))))

(deftest one-peer-not-in-multiple-not-used
  (is
   (=
    {:j1 {:t1 [:p3] :t2 [:p1] :t3 [:p2]}}
    (:allocations
     (reconfigure-cluster-workload
      {:messaging {:onyx.messaging/impl :aeron}
       :job-scheduler :onyx.job-scheduler/balanced
       :task-schedulers {:j1 :onyx.task-scheduler/colocated}
       :peers [:p1 :p2 :p3 :p4]
       :jobs [:j1]
       :tasks {:j1 [:t1 :t2 :t3]}
       :peer-sites {:p1 {:aeron/external-addr :a}
                    :p2 {:aeron/external-addr :a}
                    :p3 {:aeron/external-addr :a}
                    :p4 {:aeron/external-addr :a}}})))))

(deftest two-peers-not-in-multiple-not-used
  (is
   (=
    {:j1 {:t1 [:p3] :t2 [:p1] :t3 [:p2]}}
    (:allocations
     (reconfigure-cluster-workload
      {:messaging {:onyx.messaging/impl :aeron}
       :job-scheduler :onyx.job-scheduler/balanced
       :task-schedulers {:j1 :onyx.task-scheduler/colocated}
       :peers [:p1 :p2 :p3 :p4 :p5]
       :jobs [:j1]
       :tasks {:j1 [:t1 :t2 :t3]}
       :peer-sites {:p1 {:aeron/external-addr :a}
                    :p2 {:aeron/external-addr :a}
                    :p3 {:aeron/external-addr :a}
                    :p4 {:aeron/external-addr :a}
                    :p5 {:aeron/external-addr :a}}})))))
