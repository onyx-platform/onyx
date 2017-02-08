(ns onyx.scheduler.scheduler-test
  (:require [clojure.test :refer :all]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.generators :refer [one-group]]
            [onyx.api]))

(deftest ^:broken jitter-on-no-change
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
          (one-group
           {:jobs [:j1 :j2]
            :allocations initial-allocations
            :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10]
            :tasks {:j1 [:t1 :t2 :t3]
                    :j2 [:t4 :t5]}
            :saturation {:j1 5 :j2 5}
            :task-schedulers {:j1 :onyx.task-scheduler/balanced
                              :j2 :onyx.task-scheduler/balanced}
            :job-scheduler :onyx.job-scheduler/balanced
            :messaging {:onyx.messaging/impl :aeron}})))))))

(deftest ^:broken jitter-on-add-peer
  (is
   (= {:j1 {:t1 [:p4 :p5] :t2 [:p2] :t3 [:p7]}
       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1 :j2]
          :allocations {:j1 {:t1 [:p4] :t2 [:p2] :t3 [:p7]}
                        :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p1]}}
          :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7]
          :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken underwhelm-peers
  (is
   (= {:j1 {:t1 [:p1]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {:j1 {:t1 []}}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1]}
          :task-saturation {:j1 {:t1 1}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken not-enough-peers
  (is
   (= {}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1]}
          :min-required-peers {:j1 {:t1 10}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken only-one-job-allocated
  (is
   (= {:j1 {:t1 [:p1 :p2 :p3]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1 :j2]
          :allocations {:j1 {:t1 []} :j2 {:t2 []}}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1] :j2 [:t2]}
          :min-required-peers {:j1 {:t1 3} :j2 {:t2 3}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken even-distribution
  (is
   (= {:j1 {:t1 [:p5] :t2 [:p2] :t3 [:p1]}
       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p4]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1 :j2]
          :allocations {:j1 {:t1 []} :j2 {:t2 []}}
          :peers [:p1 :p2 :p3 :p4 :p5 :p6]
          :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken prefer-earlier-job
  (is
   (= {:j1 {:t1 [:p5 :p7] :t2 [:p2] :t3 [:p1]}
       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p4]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1 :j2]
          :allocations {:j1 {:t1 []} :j2 {:t2 []}}
          :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7]
          :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken skip-overloaded-jobs
  (is
   (= {:j1 {:t1 [:p4 :p5 :p6]}
       :j3 {:t3 [:p1 :p2 :p3]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1 :j2 :j3]
          :peers [:p1 :p2 :p3 :p4 :p5 :p6]
          :tasks {:j1 [:t1] :j2 [:t2] :j3 [:t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced
                            :j3 :onyx.task-scheduler/balanced}
          :min-required-peers {:j2 {:t2 100}}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken big-assignment
  (let [peers (map #(keyword (str "p" %)) (range 100))
        replica (reconfigure-cluster-workload
                 (one-group
                  {:jobs [:j1]
                   :allocations {}
                   :peers peers
                   :tasks {:j1 [:t1]}
                   :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                   :job-scheduler :onyx.job-scheduler/balanced
                   :messaging {:onyx.messaging/impl :aeron}}))]
    (is (= (into #{} peers)
           (into #{} (get-in replica [:allocations :j1 :t1]))))))

(deftest ^:broken grouping-sticky-peers
  (is
   (= {:j1 {:t1 [:p1 :p6] :t2 [:p2 :p3 :p4] :t3 [:p5]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {:j1 {:t1 [:p1]
                             :t2 [:p2 :p3 :p4]
                             :t3 [:p5]}}
          :peers [:p1 :p2 :p3 :p4 :p5 :p6]
          :tasks {:j1 [:t1 :t2 :t3]}
          :flux-policies {:j1 {:t2 :kill}}
          :task-saturation {:j1 {:t1 100 :t2 100 :t3 100}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken grouping-recover-flux-policy
  (is
   (= {:j1 {:t1 [:p1] :t2 [:p2 :p3 :p5] :t3 [:p4]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {:j1 {:t1 [:p1]
                             :t2 [:p2 :p3]
                             :t3 [:p4]}}
          :peers [:p1 :p2 :p3 :p4 :p5]
          :tasks {:j1 [:t1 :t2 :t3]}
          :flux-policies {:j1 {:t2 :recover}}
          :task-saturation {:j1 {:t1 100 :t2 3 :t3 100}}
          :min-required-peers {:j1 {:t2 3}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken promote-to-first-task
  (is
   (= {:j1 {:t1 [:p1 :p3] :t2 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {:j1 {:t1 [:p1]
                             :t2 [:p2 :p3]}}
          :peers [:p1 :p2 :p3]
          :tasks {:j1 [:t1 :t2]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest ^:broken percentage-grouping-task-tilt
  (is
   (= {:j1 {:t1 [:p5 :p8]
            :t2 [:p1 :p7 :p10 :p6]
            :t3 [:p2 :p4 :p3 :p9]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:job-scheduler :onyx.job-scheduler/greedy
          :task-percentages {:j1 {:t1 20 :t2 30 :t3 50}}
          :peers [:p7 :p10 :p9 :p1 :p2 :p6 :p4 :p3 :p5 :p8]
          :min-required-peers {:j1 {:t1 1 :t2 4 :t3 1}}
          :jobs [:j1]
          :tasks {:j1 [:t1 :t2 :t3]}
          :flux-policies {:j1 {:t2 :kill}}
          :messaging {:onyx.messaging/impl :aeron}
          :allocations {:j1 {}}
          :task-schedulers {:j1 :onyx.task-scheduler/percentage}
          :task-saturation {:j1 {:t1 1000 :t2 4 :t3 1000}}}))))))

(deftest ^:broken max-peers-jitter
  (is
   (= {:j1 {:t2 [:p1] :t3 [:p2] :t1 [:p5]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:job-scheduler :onyx.job-scheduler/greedy
          :saturation {:j1 3}
          :peers [:p1 :p2 :p3 :p4 :p5]
          :min-required-peers {:j1 {:t1 1 :t2 1 :t3 1}}
          :task-slot-ids {:j1 {:t2 {:p1 0} :t3 {:p2 0} :t1 {:p5 0}}}
          :jobs [:j1]
          :tasks {:j1 [:t1 :t2 :t3]}
          :messaging {:onyx.messaging/impl :aeron}
          :allocations {:j1 {:t2 [:p1] :t3 [:p2] :t1 [:p5]}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :task-saturation {:j1 {:t1 1 :t2 1 :t3 1}}}))))))
