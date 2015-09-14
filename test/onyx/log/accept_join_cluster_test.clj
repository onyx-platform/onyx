(ns onyx.log.accept-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.system]
            [onyx.log.replica :as replica]
            [schema.test]
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
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :pairs {:a :b :b :c :c :a}
                            :accepted {:a :d}
                            :peers [:a :b :c]
                            :job-scheduler :onyx.job-scheduler/greedy})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= (get-in new-replica [:pairs :a]) :d))
    (is (= (get-in new-replica [:pairs :d]) :b))
    (is (= (get-in new-replica [:accepted]) {}))
    (is (= (last (get-in new-replica [:peers])) :d))
    (is (= diff {:observer :a :subject :d}))
    (is (= (rep-reactions old-replica new-replica diff {}) []))))

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
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :pairs {}
                            :accepted {:a :d}
                            :peers [:a]
                            :job-scheduler :onyx.job-scheduler/greedy})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= (get-in new-replica [:pairs :d]) :a))
    (is (= (get-in new-replica [:pairs :a]) :d))
    (is (= (get-in new-replica [:accepted]) {}))
    (is (= (last (get-in new-replica [:peers])) :d))
    (is (= diff {:observer :a :subject :d}))
    (is (= (rep-reactions old-replica new-replica diff {}) []))))
