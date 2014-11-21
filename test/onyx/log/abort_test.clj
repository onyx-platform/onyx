(ns onyx.log.abort-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :abort-join-cluster {:id :d}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {:a :b :b :c :c :a} :prepared {:a :d} :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:pairs new-replica) => {:a :b :b :c :c :a})
  (fact (:peers new-replica) => [:a :b :c])
  (fact diff => {:aborted :d})
  (fact reactions => [{:fn :prepare-join-cluster :args {:joiner :d}}]))

(def old-replica {:pairs {:a :b :b :c :c :a} :accepted {:a :d} :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:pairs new-replica) => {:a :b :b :c :c :a})
  (fact (:peers new-replica) => [:a :b :c])
  (fact diff => {:aborted :d})
  (fact reactions => [{:fn :prepare-join-cluster :args {:joiner :d}}]))

