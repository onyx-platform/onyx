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
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:d :a})
  (fact diff => {:observer :d :subject :a})
  (fact reactions => [{:fn :notify-watchers :args {:observer :c :subject :d}}]))

(let [old-replica (assoc-in old-replica [:prepared :e] :a)
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:e :a :d :b})
  (fact diff => {:observer :d :subject :b})
  (fact reactions => [{:fn :notify-watchers :args {:observer :a :subject :d}}]))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :e] :a)
                      (assoc-in [:prepared :f] :b))
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:e :a :f :b :d :c})
  (fact diff => {:observer :d :subject :c})
  (fact reactions => [{:fn :notify-watchers :args {:observer :b :subject :d}}]))

(let [old-replica (-> old-replica
                      (assoc-in [:prepared :e] :a)
                      (assoc-in [:prepared :f] :b)
                      (assoc-in [:prepared :g] :c))
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact (:prepared new-replica) => {:e :a :f :b :g :c})
  (fact diff => nil)
  (fact reactions => nil))

(let [old-replica {:peers []}
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d})]
  (fact new-replica => {:peers [:d]})
  (fact diff => {:instant-join :d})
  (fact reactions => nil))

