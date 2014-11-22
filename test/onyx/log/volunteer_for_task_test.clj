(ns onyx.log.volunteer-for-task-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
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
                   :allocations {:j1 {:t1 [:y]}}
                   :last-allocated :j1}
      new-replica (f old-replica)]
  (fact new-replica => {:job-scheduler :onyx.job-scheduler/round-robin
                        :jobs [:j1 :j2]
                        :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
                        :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                          :j2 :onyx.task-scheduler/greedy}
                        :allocations {:j1 {:t1 [:y]} :j2 {:t4 [:x]}}
                        :last-allocated :j2}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                   :jobs [:j1 :j2]
                   :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy
                                     :j2 :onyx.task-scheduler/greedy}
                   :allocations {:j1 {:t1 [:y]} :j2 {:t4 [:z]}}
                   :last-allocated :j1}
      new-replica (f old-replica)]
  (fact new-replica =>
        {:job-scheduler :onyx.job-scheduler/round-robin
         :jobs [:j1 :j2]
         :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5]}
         :task-schedulers {:j1 :onyx.task-scheduler/greedy
                           :j2 :onyx.task-scheduler/greedy}
         :allocations {:j1 {:t1 [:y]} :j2 {:t4 [:z :x]}}
         :last-allocated :j2}))

(let [old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                   :jobs [:j1]
                   :tasks {:j1 [:t1 :t2 :t3]}
                   :allocations {:j1 {:t1 [:a :b :c]}}
                   :task-schedulers {:j1 :onyx.task-scheduler/greedy}}
      new-replica (f old-replica)]
  (fact new-replica =>
        {:job-scheduler :onyx.job-scheduler/round-robin
         :jobs [:j1]
         :tasks {:j1 [:t1 :t2 :t3]}
         :allocations {:j1 {:t1 [:a :b :c :x]}}
         :task-schedulers {:j1 :onyx.task-scheduler/greedy}
         :last-allocated :j1}))

