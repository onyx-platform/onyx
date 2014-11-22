(ns onyx.log.submit-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :submit-job {:id :a}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (:jobs new-replica) => [:a])
  (fact diff => {:job :a}))

(let [old-replica {:jobs [:b]}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (:jobs new-replica) => [:b :a])
  (fact diff => {:job :a}))

