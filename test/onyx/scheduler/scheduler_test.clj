(ns onyx.scheduler.scheduler-test
  (:require [clojure.test :refer :all]
            [onyx.scheduling.common-job-scheduler :refer [reconfigure-cluster-workload]]
            [onyx.log.generators :refer [one-group]]
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
          (one-group
           {:jobs [:j1 :j2]
            :allocations initial-allocations
            :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10]
            :peer-state {:p1 :active :p2 :active :p3 :active
                         :p4 :active :p5 :active :p6 :active
                         :p7 :active :p8 :active :p9 :active
                         :p10 :active}
            :tasks {:j1 [:t1 :t2 :t3]
                    :j2 [:t4 :t5]}
            :saturation {:j1 5 :j2 5}
            :task-schedulers {:j1 :onyx.task-scheduler/balanced
                              :j2 :onyx.task-scheduler/balanced}
            :job-scheduler :onyx.job-scheduler/balanced
            :messaging {:onyx.messaging/impl :aeron}})))))))

(deftest jitter-on-add-peer
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
          :peer-state {:p1 :active :p2 :active :p3 :active
                       :p4 :active :p5 :idle :p6 :active
                       :p7 :active}
          :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced
                            :j2 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest underwhelm-peers
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

(deftest not-enough-peers
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

(deftest only-one-job-allocated
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

(deftest even-distribution
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

(deftest prefer-earlier-job
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

(deftest skip-overloaded-jobs
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

(deftest big-assignment
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

(deftest grouping-sticky-peers
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
          :peer-state {:p1 :active :p2 :active :p3 :active
                       :p4 :active :p5 :active :p6 :idle}
          :tasks {:j1 [:t1 :t2 :t3]}
          :flux-policies {:j1 {:t2 :kill}}
          :task-saturation {:j1 {:t1 100 :t2 100 :t3 100}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest grouping-recover-flux-policy
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
          :peer-state {:p1 :active :p2 :active :p3 :active
                       :p4 :active :p5 :idle}
          :tasks {:j1 [:t1 :t2 :t3]}
          :flux-policies {:j1 {:t2 :recover}}
          :task-saturation {:j1 {:t1 100 :t2 3 :t3 100}}
          :min-required-peers {:j1 {:t2 3}}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest change-peer-state-for-moved-peers
  (is
   (= {:p1 :active :p2 :active :p3 :active
       :p4 :active :p5 :active :p6 :idle}
      (:peer-state
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {:j1 {:t1 [:p1 :p2]
                             :t2 [:p3 :p4]
                             :t3 [:p5]}}
          :peers [:p1 :p2 :p3 :p4 :p5 :p6]
          :peer-state {:p1 :active :p2 :active :p3 :active
                       :p4 :active :p5 :active :p6 :idle}
          :tasks {:j1 [:t1 :t2 :t3]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest promote-to-first-task
  (is
   (= {:j1 {:t1 [:p1 :p3] :t2 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        (one-group
         {:jobs [:j1]
          :allocations {:j1 {:t1 [:p1]
                             :t2 [:p2 :p3]}}
          :peers [:p1 :p2 :p3]
          :peer-state {:p1 :active :p2 :active :p3 :active}
          :tasks {:j1 [:t1 :t2]}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :job-scheduler :onyx.job-scheduler/balanced
          :messaging {:onyx.messaging/impl :aeron}}))))))

(deftest percentage-grouping-task-tilt
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

(deftest max-peers-jitter
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
          :peer-state {:p1 :active :p2 :idle :p3 :idle :p4 :idle :p5 :idle}
          :task-schedulers {:j1 :onyx.task-scheduler/balanced}
          :task-saturation {:j1 {:t1 1 :t2 1 :t3 1}}}))))))
