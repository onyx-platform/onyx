(ns onyx.log.killed-job-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.system]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-killed-job
  (let [entry (create-log-entry :kill-job {:job :j1})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :job-scheduler :onyx.job-scheduler/greedy
                            :jobs [:j1]
                            :tasks {:j1 [:t1 :t2]}
                            :allocations {:j1 {:t1 [:a :b] :t2 [:c]}}
                            :task-metadata {:j1 {:t1 [:task-data]}}
                            :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                            :peer-state {:a :active :b :active :c :active}
                            :peers [:a :b :c]})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        a-reactions (rep-reactions old-replica new-replica diff {:id :a})
        d-reactions (rep-reactions old-replica new-replica diff {:id :d})]
    (is (= (:killed-jobs new-replica) [:j1]))
    (is (= (:task-metadata new-replica) {}))
    (is (= (get-in new-replica [:allocations :j1]) nil))
    (is (= (get-in new-replica [:peer-state :a]) :idle))
    (is (= (get-in new-replica [:peer-state :b]) :idle))
    (is (= (get-in new-replica [:peer-state :c]) :idle))

    (is (= diff #{:j1}))
    (is (= a-reactions []))
    (is (= d-reactions []))))
