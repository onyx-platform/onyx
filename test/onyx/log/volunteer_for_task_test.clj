(ns onyx.log.volunteer-for-task-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry
  (create-log-entry :volunteer-for-task
                    {:id :x :job-scheduler :onyx.job-scheduler/greedy}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:jobs [:a]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:allocations new-replica) => {:a [:x]}))

(let [old-replica {:jobs [:a] :allocations {:a [:y]}}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:allocations new-replica) => {:a [:y :x]}))

