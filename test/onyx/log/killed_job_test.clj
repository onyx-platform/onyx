(ns onyx.log.killed-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :kill-job {:job :j1}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/greedy
                  :jobs [:j1]
                  :tasks {:j1 [:t1 :t2]}
                  :allocations {:j1 {:t1 [:a :b] :t2 [:c]}}
                  :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
                  :peer-state {:a :active :b :active :c :active}
                  :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      a-reactions (rep-reactions old-replica new-replica diff {:id :a})
      d-reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:killed-jobs new-replica) => [:j1])
  (fact (get-in new-replica [:allocations :j1]) => nil)
  (fact (get-in new-replica [:peer-state :a]) => :idle)
  (fact (get-in new-replica [:peer-state :b]) => :idle)
  (fact (get-in new-replica [:peer-state :c]) => :idle)

  (fact diff => #{:j1})

  (fact a-reactions => [{:fn :volunteer-for-task :args {:id :a}}])
  (fact d-reactions => nil))

