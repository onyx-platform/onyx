(ns onyx.log.submit-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [onyx.log.replica :as replica]
            [clojure.test :refer [deftest is testing]]))

(def entry (create-log-entry :submit-job {:id :a :tasks [:t1]
                                          :task-scheduler :onyx.task-scheduler/balanced
                                          :saturation 42}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica (merge replica/base-replica
                        {:messaging {:onyx.messaging/impl :dummy-messenger}
                         :job-scheduler :onyx.job-scheduler/greedy
                         :peers [:p1]}))

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:jobs new-replica) => [:a])
  (fact diff => {:job :a}))

(let [old-replica (merge replica/base-replica 
                         {:messaging {:onyx.messaging/impl :dummy-messenger}
                          :job-scheduler :onyx.job-scheduler/greedy :jobs [:b]
                          :task-schedulers {:b :onyx.task-scheduler/balanced}
                          :tasks {:b [:t1 :t2]}})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :x})]
  (fact (:jobs new-replica) => [:b :a])
  (fact diff => {:job :a}))

(def entry (create-log-entry :submit-job {:id :j1
                                          :task-scheduler :onyx.task-scheduler/balanced
                                          :saturation 42
                                          :tasks [:t1 :t2 :t3]}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica (merge replica/base-replica
                        {:messaging {:onyx.messaging/impl :dummy-messenger}
                         :job-scheduler :onyx.job-scheduler/balanced
                         :peers [:p1 :p2]}))

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (rep-reactions old-replica new-replica diff {:id :p1}) => [])
  (fact (rep-reactions old-replica new-replica diff {:id :p2}) => []))
