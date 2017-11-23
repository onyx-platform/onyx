(ns onyx.log.killed-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.log.generators :refer [one-group]]
            [onyx.system]
            [schema.test]
            [onyx.peer.log-version]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-killed-job
  (let [entry (create-log-entry :kill-job {:job :j1})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica
                           (one-group
                            {:messaging {:onyx.messaging/impl :aeron}
                             :job-scheduler :onyx.job-scheduler/greedy
                             :log-version onyx.peer.log-version/version
                             :jobs [:j1]
                             :tasks {:j1 [:t1 :t2]}
                             :allocations {:j1 {:t1 [:a :b] :t2 [:c]}}
                             :task-metadata {:j1 {:t1 [:task-data]}}
                             :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                             :peers [:a :b :c]}))
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        a-reactions (rep-reactions old-replica new-replica diff {:id :a :type :group})
        d-reactions (rep-reactions old-replica new-replica diff {:id :d :type :group})]
    (is (= [:j1] (:killed-jobs new-replica)))
    (is (= {} (:task-metadata new-replica)))
    (is (= nil (get-in new-replica [:allocations :j1])))
    (is (= #{:j1} diff))
    (is (= [] a-reactions))
    (is (= [] d-reactions))))
