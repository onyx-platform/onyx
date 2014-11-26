(ns onyx.log.complete-task-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :complete-task {:job :j1 :task :t1}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/greedy
                  :jobs [:j1]
                  :tasks {:j1 [:t1 :t2]}
                  :allocations {:j1 {:t1 [:a :b] :t2 [:c]}}
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
                  :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (:completions new-replica) => {:j1 [:t1]})
  (fact (:allocations new-replica) => {:j1 {:t2 [:c]}})
  (fact (rep-reactions old-replica new-replica diff {:id :a}) =>
        [{:fn :volunteer-for-task :args {:id :a}}])
  (fact (rep-reactions old-replica new-replica diff {:id :b}) =>
        [{:fn :volunteer-for-task :args {:id :b}}])
  (fact (rep-reactions old-replica new-replica diff {:id :c}) => nil?))

