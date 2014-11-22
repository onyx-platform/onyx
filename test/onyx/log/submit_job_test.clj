(ns onyx.log.submit-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :submit-job {:id :a :job-scheduler :onyx.job-scheduler/greedy}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:jobs new-replica) => [:a])
  (fact diff => {:job :a})
  (fact reactions => [{:fn :volunteer-for-task
                       :args {:id :x :job-scheduler :onyx.job-scheduler/greedy}}]))

(let [old-replica {:jobs [:b]}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:jobs new-replica) => [:b :a])
  (fact diff => {:job :a})
  (fact reactions => []))

