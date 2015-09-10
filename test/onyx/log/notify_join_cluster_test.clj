(ns onyx.log.notify-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.test-helper :refer [load-config]]
            [onyx.log.replica :as replica]
            [onyx.system]
            [clojure.test :refer [deftest is]]))

(def peer-config
  (:peer-config (load-config)))

(let [entry (create-log-entry :notify-join-cluster {:observer :d :subject :a})
      f (partial extensions/apply-log-entry entry)
      rep-diff (partial extensions/replica-diff entry)
      rep-reactions (partial extensions/reactions entry)
      old-replica (merge replica/base-replica 
                         {:messaging {:onyx.messaging/impl :dummy-messenger}
                          :job-scheduler :onyx.job-scheduler/greedy
                          :pairs {:a :b :b :c :c :a} :prepared {:a :d} :peers [:a :b :c]})
      new-replica (f old-replica)
      diff (rep-diff old-replica new-replica)
      reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
  (fact diff => {:observer :d :subject :b :accepted-joiner :d :accepted-observer :a})
  (fact reactions => [{:fn :accept-join-cluster :args diff :immediate? true}])
  (fact (rep-reactions old-replica new-replica diff {:id :a}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :b}) => nil)
  (fact (rep-reactions old-replica new-replica diff {:id :c}) => nil))
