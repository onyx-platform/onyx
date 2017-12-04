(ns onyx.log.flux-policy-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.log.generators :refer [one-group]]
            [onyx.system]
            [schema.test]
            [onyx.peer.log-version]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest flux-policy-leave-tests
  (testing "Flux policy continue" 
    (let [entry (create-log-entry :leave-cluster {:id :c :group-id :g1})
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
                               :allocations {:j1 {:t1 [:a] :t2 [:b :c]}}
                               :flux-policies {:j1 {:t2 :kill}}
                               :min-required-peers {:j1 {:t1 1 :t2 2}}
                               :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                               :task-saturation {:j1 {:t1 42 :t2 42}}
                               :peers [:a :b :c]}))
          new-replica (f old-replica)]
      (is (= [:j1] (:killed-jobs new-replica)))
      (is (= [:j1] (get-in new-replica [:killed-jobs])))))

  (testing "Flux policy kill" 
    (let [entry (create-log-entry :leave-cluster {:id :c :group-id :g1})
          f (partial extensions/apply-log-entry entry)
          old-replica (merge replica/base-replica
                             (one-group
                              {:messaging {:onyx.messaging/impl :aeron}
                               :job-scheduler :onyx.job-scheduler/greedy
                               :log-version onyx.peer.log-version/version
                               :jobs [:j1]
                               :tasks {:j1 [:t1 :t2]}
                               :allocations {:j1 {:t1 [:a] :t2 [:b :c]}}
                               :flux-policies {:j1 {:t2 :continue}}
                               :min-required-peers {:j1 {:t1 1 :t2 2}}
                               :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                               :task-saturation {:j1 {:t1 42 :t2 42}}
                               :peers [:a :b :c]}))
          new-replica (f old-replica)]
      (is (= [:j1] (:jobs new-replica)))
      (is (= [:j1] (get-in new-replica [:jobs]))))))
