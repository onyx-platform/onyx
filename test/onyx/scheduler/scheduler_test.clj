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
         :allocations {}
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
   (= {:j1 {:t1 [:p5] :t2 [:p2] :t3 [:p1]}
       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p4]}}
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
   (= {:j1 {:t1 [:p5 :p7] :t2 [:p2] :t3 [:p1]}
       :j2 {:t4 [:p6] :t5 [:p3] :t6 [:p4]}}
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

(deftest big-assignment
  (let [peers (map #(keyword (str "p" %)) (range 100))
        replica (reconfigure-cluster-workload
                 {:jobs [:j1]
                  :allocations {}
                  :peers peers
                  :tasks {:j1 [:t1]}
                  :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                  :job-scheduler :onyx.job-scheduler/balanced
                  :messaging {:onyx.messaging/impl :aeron}})]
    (is (= (into #{} peers)
           (into #{} (get-in replica [:allocations :j1 :t1]))))))

(deftest grouping-sticky-peers
  (is
   (= {:j1 {:t1 [:p1 :p6] :t2 [:p2 :p3 :p4] :t3 [:p5]}}
      (:allocations
       (reconfigure-cluster-workload
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
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest grouping-recover-flux-policy
  (is
   (= {:j1 {:t1 [:p1] :t2 [:p2 :p3 :p5] :t3 [:p4]}}
      (:allocations
       (reconfigure-cluster-workload
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
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest change-peer-state-for-moved-peers
  (is
   (= {:p1 :active :p2 :active :p3 :active
       :p4 :active :p5 :active :p6 :idle}
      (:peer-state
       (reconfigure-cluster-workload
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
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest promote-to-first-task
  (is
   (= {:j1 {:t1 [:p1 :p3] :t2 [:p2]}}
      (:allocations
       (reconfigure-cluster-workload
        {:jobs [:j1]
         :allocations {:j1 {:t1 [:p1]
                            :t2 [:p2 :p3]}}
         :peers [:p1 :p2 :p3]
         :tasks {:j1 [:t1 :t2]}
         :task-schedulers {:j1 :onyx.task-scheduler/balanced}
         :job-scheduler :onyx.job-scheduler/balanced
         :messaging {:onyx.messaging/impl :aeron}})))))

(deftest percentage-grouping-task-tilt
  (is
   (= {:j1 {:t1 [:p5 :p8]
            :t2 [:p1 :p7 :p10 :p6]
            :t3 [:p2 :p4 :p3 :p9]}}
      (:allocations
       (reconfigure-cluster-workload
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
         :task-saturation {:j1 {:t1 1000 :t2 4 :t3 1000}}})))))

(deftest max-peers-jitter
  (is
   (= {:j1 {:t2 [:p1] :t3 [:p2] :t1 [:p5]}}
      (:allocations
       (reconfigure-cluster-workload
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
         :task-saturation {:j1 {:t1 1 :t2 1 :t3 1}}})))))

;; Bug #503
(deftest state-churn
  (let [replica {:output-tasks {:j1 [:t8]}
                 :state-logs-marked #{}
                 :job-scheduler :onyx.job-scheduler/balanced
                 :ackers {:j1 [:p10]}
                 :saturation {:j1 Double/POSITIVE_INFINITY}
                 :task-percentages {}
                 :state-logs {:j1 {:t7 {0 [23]}}}
                 :peers [:p1 :p2 :p3 :p4 :p5 :p6 :p7 :p8 :p9 :p10 :p11
                         :p12 :p13 :p14 :p15 :p16]
                 :acker-exclude-outputs {:j1 false}
                 :min-required-peers {:j1 {:t1 1 :t2 1 :t3 1 :t4 1
                                           :t5 1 :t6 1 :t7 1 :t8 1}}
                 :task-slot-ids {:j1 {:t8 {:p5 1 :p4 3 :p9 0 :p13 2 :p15 4}
                                      :t6 {:p2 0 :p7 2 :p8 3 :p6 4 :p14 1}
                                      :t2 {:p10 0}
                                      :t3 {:p12 0}
                                      :t5 {:p11 0}
                                      :t7 {:p16 0}
                                      :t4 {:p3 0}
                                      :t1 {:p1 0}}}
                 :accepted {}
                 :jobs [:j1]
                 :tasks {:j1 [:t1 :t2 :t3 :t4 :t5 :t6 :t7 :t8]}
                 :task-metadata {}
                 :exhausted-inputs {}
                 :pairs {:p16 :p7 :p14 :p6 :p5 :p1 :p2 :p16 :p12 :p3
                         :p15 :p4 :p9 :p13 :p7 :p5 :p8 :p15 :p6 :p11
                         :p13 :p14 :p11 :p10 :p10 :p8 :p3 :p9 :p1 :p12 :p4 :p2}
                 :flux-policies {:j1 {}}
                 :messaging {:onyx.messaging/impl :aeron}
                 :allocations {:j1 {:t7 [:p16]
                                    :t6 [:p14 :p2 :p7 :p8 :p6]
                                    :t8 [:p5 :p15 :p9 :p13 :p4]
                                    :t3 [:p12]
                                    :t5 [:p11]
                                    :t2 [:p10]
                                    :t4 [:p3]
                                    :t1 [:p1]}}
                 :input-tasks {:j1 [:t1 :t2 :t3 :t4 :t5]}
                 :acker-percentage {:j1 1}
                 :peer-state {:p16 :idle
                              :p14 :active
                              :p5 :active
                              :p2 :active
                              :p12 :active
                              :p15 :idle
                              :p9 :active
                              :p7 :active
                              :p8 :active
                              :p6 :active
                              :p13 :active
                              :p11 :active
                              :p10 :active
                              :p3 :active
                              :p1 :active
                              :p4 :active}
                 :acker-exclude-inputs {:j1 false}
                 :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                 :task-saturation {:j1 {:t1 1 :t2 1 :t3 1 :t4 1 :t5 1
                                        :t6 Double/POSITIVE_INFINITY
                                        :t7 1
                                        :t8 Double/POSITIVE_INFINITY}}}
        entry {:fn :leave-cluster
               :args {:id :p10}
               :entry-parent 91
               :peer-parent :p11
               :message-id 126
               :created-at 1453666030003}
        results (:allocations (onyx.extensions/apply-log-entry entry replica))]
    (is (= {:j1 {:t1 [:p1]
                 :t2 [:p15]
                 :t3 [:p12]
                 :t4 [:p3]
                 :t5 [:p11]
                 :t6 [:p2 :p8 :p7 :p14 :p6]
                 :t7 [:p16]
                 :t8 [:p4 :p5 :p9 :p13]}}
           results))))
