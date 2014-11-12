(ns onyx.accept-join-cluster-test
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

(def rep-diff (partial extensions/replica-diff :prepare-join-cluster))

(def rep-reactions (partial extensions/reactions :prepare-join-cluster))

(def old-replica {:pairs {:a :b :b :c :c :a} :accepted {:d :a} :peers [:a :b :c]})

(let [new-replica (f old-replica 0)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:d :a})
  (fact diff => {:observer :d :subject :a})
  (fact reactions => [{:fn :notify-watchers :args {:observer :c :subject :d}}]))

