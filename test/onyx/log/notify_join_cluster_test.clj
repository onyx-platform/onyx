(ns onyx.log.notify-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.log.replica :as replica]
            [onyx.system]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(deftest log-notify-join-cluster 
  (let [peer-config (:peer-config (load-config))
        entry (create-log-entry :notify-join-cluster {:observer :d :subject :a})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :aeron}
                            :job-scheduler :onyx.job-scheduler/greedy
                            :pairs {:a :b :b :c :c :a} :prepared {:a :d} :groups [:a :b :c]})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions 
                   old-replica new-replica diff {:id :d :type :group})]
    (is (= {:observer :d :subject :b :accepted-joiner :d :accepted-observer :a} diff))
    (is (= [{:fn :accept-join-cluster :args diff}] reactions))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :a :type :group})))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :b :type :group})))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :c :type :group})))))
