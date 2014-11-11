(ns onyx.notify-watchers-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :notify-watchers {:watching :a :watched :d}))

(def f (extensions/apply-log-entry (:fn entry) (:args entry)))

(def rep-diff (partial extensions/replica-diff :notify-watchers))

(def rep-reactions (partial extensions/reactions :notify-watchers))

(def old-replica {:pairs {:a :b :b :c :c :a} :prepared {:d :a} :peers [:a :b :c]})

(let [new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :c})]
  (fact diff => {:watching :d :watched :a})
  (fact reactions => [{:f :accept-join-cluster
                       :args {:accepted {:watching :d
                                         :watched :a}
                              :updated-watch {:watching :c
                                              :watched :d}}}])
  (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :b}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :d}) => nil))

