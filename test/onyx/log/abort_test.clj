(ns onyx.log.abort-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.system]
            [onyx.log.replica :as replica]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [schema.core :as s]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-abort-test
  (let [peer-state {:id :d :messenger (dummy-messenger {:onyx.peer/try-join-once? false})}
        entry (create-log-entry :abort-join-cluster {:id :d})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)

        old-replica (merge replica/base-replica 
                           {:job-scheduler :onyx.job-scheduler/balanced
                            :messaging {:onyx.messaging/impl :dummy-messenger}
                            :pairs {:a :b :b :c :c :a} 
                            :prepared {:a :d} 
                            :peers [:a :b :c]})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff peer-state)]
    (is (= {:a :b :b :c :c :a} (:pairs new-replica)))
    (is (= [:a :b :c] (:peers new-replica)))
    (is (= {:aborted :d} diff))
    (is (= [{:fn :prepare-join-cluster
             :args {:joiner :d
                    :peer-site {:address 1}}}] 
           reactions))))
