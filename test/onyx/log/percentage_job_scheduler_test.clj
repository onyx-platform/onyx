(ns onyx.log.percentage-job-scheduler-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.commands.common :refer [reallocate-from-job?]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :submit-job {:id :a :percentage 70}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(let [old-replica {:job-scheduler :onyx.job-scheduler/percentage
                   :peers [:x]}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:a (:percentages new-replica)) => 70)
  (fact reactions => [{:fn :volunteer-for-task :args {:id :x}}]))

(fact
 (reallocate-from-job? :onyx.job-scheduler/percentage nil
                       {:jobs [:a :b :c]
                        :peers [:p1]
                        :percentages {:a 70 :b 30 :c 20}}
                       :p1)
 => true)

(fact
 (reallocate-from-job? :onyx.job-scheduler/percentage nil
                       {:jobs [:a :b :c]
                        :peers [:p1]
                        :allocations {:a {:t1 [:p1]}}
                        :percentages {:a 70 :b 30 :c 20}}
                       {:id :p1})
 => nil)

(fact
 (reallocate-from-job? :onyx.job-scheduler/percentage nil
                       {:jobs [:a :b :c]
                        :tasks {:a [:t1] :b [:t2] :c [:t3]}
                        :task-schedulers {:a :onyx.task-scheduler/round-robin
                                          :b :onyx.task-scheduler/round-robin
                                          :c :onyx.task-scheduler/round-robin}
                        :peers [:p1 :p2 :p3 :p4 :p5]
                        :allocations {:a {:t1 [:p1 :p2 :p3 :p4 :p5]}}
                        :percentages {:a 70 :b 30 :c 20}}
                       {:id :p5})
 => true)

