(ns onyx.log.leave-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :leave-cluster {:id :c}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (get-in new-replica [:pairs :a]) => :b)
  (fact (get-in new-replica [:pairs :b]) => :a)
  (fact (get-in new-replica [:pairs :c]) => nil)
  (fact diff => {:died :c :updated-watch {:observer :b :subject :a}})
  (fact (rep-reactions old-replica new-replica diff {}) => []))

(def entry (create-log-entry :leave-cluster {:id :b}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {:a :b :b :a} :peers [:a :b]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)]
  (fact (get-in new-replica [:pairs :a]) => nil)
  (fact (get-in new-replica [:pairs :b]) => nil)
  (fact diff => {:died :b :updated-watch {:observer :a :subject :a}})
  (fact (rep-reactions old-replica new-replica diff {}) => []))



(def entry (create-log-entry :leave-cluster {:id :b}))

(def f (partial extensions/apply-log-entry entry))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {} :prepared {:a :b} :peers [:b]})

(f old-replica)
(rep-reactions old-replica (f old-replica) (rep-diff old-replica (f old-replica)) {:id :b})

