(ns onyx.log.volunteer-for-task-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :volunteer-for-task {:id :x}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(let [old-replica {:job-scheduler :onyx.job-scheduler/greedy
                   :jobs [:j1]
                   :tasks {:j1 [:t1 :t2 :t3]}
                   :allocations {:j1 {:t1 [:y]}}
                   :ackers {:j1 [:x]}
                   :acker-percentage {:j1 5}
                   :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
                   :peers [:x :y]}
      new-replica (f old-replica)]
  (fact new-replica =>
        {:job-scheduler :onyx.job-scheduler/greedy
         :jobs [:j1]
         :tasks {:j1 [:t1 :t2 :t3]}
         :allocations {:j1 {:t1 [:y] :t2 [:x]}}

         :ackers {:j1 [:x]}
         :acker-percentage {:j1 5}
         :task-schedulers {:j1 :onyx.task-scheduler/round-robin}
         :peer-state {:x :warming-up}
         :peers [:x :y]})
  (extensions/replica-diff entry old-replica new-replica))

