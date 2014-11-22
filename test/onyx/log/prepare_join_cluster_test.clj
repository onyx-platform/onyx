(ns onyx.log.prepare-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [midje.sweet :refer :all]))

(def entry (create-log-entry :prepare-join-cluster {:joiner :d}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica {:pairs {:a :b :b :c :c :a} :peers [:a :b :c]})

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :a})]
  (fact (:prepared new-replica) => {:a :d})
  (fact diff => {:observer :a :subject :d})
  (fact reactions => [{:fn :notify-join-cluster
                       :args {:observer :d :subject :b}
                       :immediate? true}]))

(let [old-replica (assoc-in old-replica [:prepared :a] :e)
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :b})]
  (fact (:prepared new-replica) => {:a :e :b :d})
  (fact diff => {:observer :b :subject :d})
  (fact reactions => [{:fn :notify-join-cluster
                       :args {:observer :d :subject :c}
                       :immediate? true}]))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :a] :e)
                      (assoc-in [:prepared :b] :f)
                      (assoc-in [:prepared :c] :g))
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:a :e :b :f :c :g})
  (fact diff => nil)
  (fact reactions => [{:fn :abort-join-cluster
                       :args {:id :d}
                       :immediate? true}]))

(let [old-replica {:peers []}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact new-replica => {:peers [:d]})
  (fact diff => {:instant-join :d})
  (fact reactions => nil))

(let [old-replica {:peers [:a]}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :a})]
  (fact new-replica => {:peers [:a] :prepared {:a :d}})
  (fact diff => {:observer :a :subject :d})
  (fact reactions => [{:fn :notify-join-cluster
                       :args {:observer :d :subject :a}
                       :immediate? true}]))

(let [old-replica {:pairs {:a :b :b :a}
                   :accepted {}
                   :prepared {:a :c}
                   :peers [:a :b]}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact new-replica => old-replica)
  (fact diff => nil)
  (fact reactions => [{:fn :abort-join-cluster
                       :args {:id :d}
                       :immediate? true}]))

