(ns onyx.log.percentage-job-scheduler-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
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

