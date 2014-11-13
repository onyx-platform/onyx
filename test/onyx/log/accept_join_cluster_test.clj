(ns onyx.log.accept-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry
  (create-log-entry :accept-join-cluster
                    {:accepted {:observer :d
                                :subject :a}
                     :updated-watch {:observer :c
                                     :subject :d}}))

(def f (extensions/apply-log-entry (:fn entry) (:args entry)))

(def rep-diff (partial extensions/replica-diff :accept-join-cluster))

(def rep-reactions (partial extensions/reactions :accept-join-cluster))

(def old-replica {:pairs {:a :b :b :c :c :a} :accepted {:d :a} :peers [:a :b :c]})

(let [new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica (:args entry))]
  (fact (get-in new-replica [:pairs :d]) => :a)
  (fact (get-in new-replica [:pairs :c]) => :d)
  (fact (get-in new-replica [:accepted]) => {})
  (fact (last (get-in new-replica [:peers])) => :d)
  (fact diff => {:observer :d :subject :a})
  (fact (rep-reactions old-replica new-replica diff {}) => []))

