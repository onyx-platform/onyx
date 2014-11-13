(ns onyx.log.leave-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :leave-cluster {:id :c}))

(def f (extensions/apply-log-entry (:fn entry) (:args entry)))

(def rep-diff (partial extensions/replica-diff (:fn entry)))

(def rep-reactions (partial extensions/reactions (:fn entry)))

(def old-replica {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]})

(let [new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica (:args entry))]
  (fact (get-in new-replica [:pairs :a]) => :b)
  (fact (get-in new-replica [:pairs :b]) => :a)
  (fact (get-in new-replica [:pairs :c]) => nil)
  (fact diff => {:died :c :updated-watch {:observer :b :subject :a}})
  (fact (rep-reactions old-replica new-replica diff {}) => []))

