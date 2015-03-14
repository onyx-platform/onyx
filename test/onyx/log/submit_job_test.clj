(ns onyx.log.submit-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :submit-job {:id :a :tasks [:t1]}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/greedy
                  :peers [:p1]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:jobs new-replica) => [:a])
  (fact diff => {:job :a})
  (fact reactions => [{:fn :volunteer-for-task :args {:id :x}}]))

(let [old-replica {:job-scheduler :onyx.job-scheduler/greedy :jobs [:b]
                   :tasks {:b [:t1 :t2]}}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:jobs new-replica) => [:b :a])
  (fact diff => {:job :a})
  (fact reactions => nil))

(def entry (create-log-entry :submit-job {:id :j1
                                          :tasks [:t1 :t2 :t3]}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                  :peers [:p1 :p2]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (rep-reactions old-replica new-replica diff {:id :p1}) => nil?)
  (fact (rep-reactions old-replica new-replica diff {:id :p2}) => nil?))

(def entry (create-log-entry :submit-job {:id :j1
                                          :tasks [:t1 :t2 :t3]}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:job-scheduler :onyx.job-scheduler/round-robin
                  :peers [:p1 :p2 :p3]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (rep-reactions old-replica new-replica diff {:id :p1}) =not=> nil?)
  (fact (rep-reactions old-replica new-replica diff {:id :p2}) =not=> nil?)
  (fact (rep-reactions old-replica new-replica diff {:id :p3}) =not=> nil?))

