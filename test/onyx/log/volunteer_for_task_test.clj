(ns onyx.log.volunteer-for-task-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry
  (create-log-entry :volunteer-for-task {:id :x}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(let [old-replica {:job-scheduler :onyx.job-scheduler/greedy
                   :jobs [:j1]
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy}
                   :tasks {:j1 [:t1 :t2 :t3]}}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:allocations new-replica) => {:j1 {:t1 [:x]}}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/greedy
                   :jobs [:j1]
                   :tasks {:j1 [:t1 :t2 :t3]}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy}
                   :allocations {:j1 {:t1 [:y]}}}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:allocations new-replica) => {:j1 {:t1 [:y :x]}}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                   :jobs [:j1 :j2]
                   :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                     :j2 :onyx.task-scheduler/greedy}
                   :allocations {:j1 {:t1 [:y]} :j2 {}}
                   :peers [:x :y]}
      new-replica (f old-replica)]
  (fact new-replica => {:job-scheduler :onyx.job-scheduler/round-robin
                        :jobs [:j1 :j2]
                        :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
                        :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                          :j2 :onyx.task-scheduler/greedy}
                        :allocations {:j1 {:t1 [:y]} :j2 {:t4 [:x]}}
                        :peer-state {:x :active} 
                        :peers [:x :y]}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                   :jobs [:j1 :j2]
                   :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                     :j2 :onyx.task-scheduler/greedy}
                   :allocations {:j1 {:t1 [:y :z]} :j2 {:t4 []}}
                   :peers [:x :y :z]}
      new-replica (f old-replica)]
  (fact new-replica =>
        {:job-scheduler :onyx.job-scheduler/round-robin
         :jobs [:j1 :j2]
         :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
         :task-schedulers {:j1 :onyx.task-scheduler/greedy
                           :j2 :onyx.task-scheduler/greedy}
         :allocations {:j1 {:t1 [:y :z]} :j2 {:t4 [:x]}}
         :peer-state {:x :active}
         :peers [:x :y :z]}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                   :jobs [:j1]
                   :tasks {:j1 [:t1 :t2 :t3]}
                   :allocations {:j1 {:t1 [:a :b :c]}}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy}
                   :peers [:a :b :c :x]}
      new-replica (f old-replica)]
  (fact new-replica =>
        {:job-scheduler :onyx.job-scheduler/round-robin
         :jobs [:j1]
         :tasks {:j1 [:t1 :t2 :t3]}
         :allocations {:j1 {:t1 [:a :b :c :x]}}
         :task-schedulers {:j1 :onyx.task-scheduler/greedy}
         :peer-state {:x :active}
         :peers [:a :b :c :x]}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/greedy
                   :jobs [:j1]
                   :tasks {:j1 [:t1 :t2 :t3]}
                   :allocations {:j1 {:t1 [:y]}}
                   :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
                   :peers [:x :y]}
      new-replica (f old-replica)]
  (fact new-replica =>
        {:job-scheduler :onyx.job-scheduler/greedy
         :jobs [:j1]
         :tasks {:j1 [:t1 :t2 :t3]}
         :allocations {:j1 {:t1 [:y] :t2 [:x]}}
         :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
         :peer-state {:x :active}
         :peers [:x :y]})
  (extensions/replica-diff entry old-replica new-replica))

(let [old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                   :jobs [:j1 :j2]
                   :tasks {:j1 [:t1] :j2 [:t2]}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                     :j2 :onyx.task-scheduler/greedy}
                   :allocations {:j1 {:t1 []} :j2 {:t2 [:y]}}
                   :sealing-task {:t1 true}
                   :peers [:x :y]}
      new-replica (f old-replica)]
  (fact new-replica => {:job-scheduler :onyx.job-scheduler/round-robin
                        :jobs [:j1 :j2]
                        :tasks {:j1 [:t1] :j2 [:t2]}
                        :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                          :j2 :onyx.task-scheduler/greedy}
                        :allocations {:j1 {:t1 []} :j2 {:t2 [:y :x]}}
                        :sealing-task {:t1 true}
                        :peer-state {:x :active}
                        :peers [:x :y]}))