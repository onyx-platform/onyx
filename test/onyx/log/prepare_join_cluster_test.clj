(ns onyx.log.prepare-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [com.stuartsierra.component :as component]
            [onyx.log.replica :as replica]
            [onyx.messaging.dummy-messenger]
            [onyx.system]
            [midje.sweet :refer :all]))

(def entry
  (create-log-entry :prepare-join-cluster {:joiner :d
                                           :peer-site {:address 1}}))

(def f (partial extensions/apply-log-entry (assoc entry :message-id 0)))

(def rep-diff (partial extensions/replica-diff entry))

(def rep-reactions (partial extensions/reactions entry))

(def old-replica (merge replica/base-replica
                        {:messaging {:onyx.messaging/impl :dummy-messenger}
                         :pairs {:a :b :b :c :c :a} :peers [:a :b :c]
                         :peer-sites {}
                         :job-scheduler :onyx.job-scheduler/balanced}))

(let [new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :a :messenger (dummy-messenger {})})]
  (fact (:prepared new-replica) => {:a :d})
  (fact diff => {:observer :a :subject :d})
  (fact reactions => [{:fn :notify-join-cluster
                       :args {:observer :d :subject :b}
                       :immediate? true}]))

(let [old-replica (assoc-in old-replica [:prepared :a] :e)
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :b :messenger (dummy-messenger {})})]
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
      reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
  (fact (:prepared new-replica) => {:a :e :b :f :c :g})
  (fact diff => nil)
  (fact reactions => [{:fn :abort-join-cluster
                       :args {:id :d}
                       :immediate? true}]))

(let [old-replica (merge replica/base-replica 
                         {:messaging {:onyx.messaging/impl :dummy-messenger}
                          :peers []
                          :job-scheduler :onyx.job-scheduler/balanced})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
  (fact (:peers new-replica) => [:d])
  (fact (:peer-state new-replica) => {:d :idle})
  (fact diff => {:instant-join :d})
  (fact reactions => nil))

(let [old-replica (merge replica/base-replica
                         {:messaging {:onyx.messaging/impl :dummy-messenger}
                          :job-scheduler :onyx.job-scheduler/greedy
                          :peers [:a]})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :a :messenger (dummy-messenger {})})]
  (fact (:peers new-replica) => [:a])
  (fact (:prepared new-replica) => {:a :d})
  (fact diff => {:observer :a :subject :d})
  (fact reactions => [{:fn :notify-join-cluster
                       :args {:observer :d :subject :a}
                       :immediate? true}]))

(let [old-replica (merge replica/base-replica
                         {:messaging {:onyx.messaging/impl :dummy-messenger}
                          :pairs {:a :b :b :a}
                          :accepted {}
                          :prepared {:a :c}
                          :peers [:a :b]
                          :job-scheduler :onyx.job-scheduler/balanced})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
  (fact new-replica => old-replica)
  (fact diff => nil)
  (fact reactions => [{:fn :abort-join-cluster
                       :args {:id :d}
                       :immediate? true}]))
