(ns onyx.log.accept-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [onyx.log.replica :as replica]
            [schema.test]
            [onyx.peer.log-version]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-accept-join-cluster-1 
  (let [entry (create-log-entry :accept-join-cluster
                                {:observer :d
                                 :subject :b
                                 :accepted-joiner :d
                                 :accepted-observer :a})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :aeron}
                            :log-version onyx.peer.log-version/version
                            :pairs {:a :b :b :c :c :a}
                            :accepted {:a :d}
                            :groups [:a :b :c]
                            :job-scheduler :onyx.job-scheduler/greedy})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= :d (get-in new-replica [:pairs :a])))
    (is (= :b (get-in new-replica [:pairs :d])))
    (is (= {} (get-in new-replica [:accepted])))
    (is (= :d (last (get-in new-replica [:groups]))))
    (is (= {:observer :a :subject :d} diff))
    (is (= [] (rep-reactions old-replica new-replica diff {:type :group})))))

(deftest log-accept-join-cluster-2 
  (let [entry (create-log-entry :accept-join-cluster
                                {:observer :d
                                 :subject :b
                                 :accepted-joiner :d
                                 :accepted-observer :a})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :aeron}
                            :log-version onyx.peer.log-version/version
                            :pairs {}
                            :accepted {:a :d}
                            :groups [:a]
                            :job-scheduler :onyx.job-scheduler/greedy})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= :a (get-in new-replica [:pairs :d])))
    (is (= :d (get-in new-replica [:pairs :a])))
    (is (= {} (get-in new-replica [:accepted])))
    (is (= :d (last (get-in new-replica [:groups]))))
    (is (= {:observer :a :subject :d} diff))
    (is (= [] (rep-reactions old-replica new-replica diff {:type :group})))))
