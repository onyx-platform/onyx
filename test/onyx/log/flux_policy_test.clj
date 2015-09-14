(ns onyx.log.flux-policy-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.system]
            [clojure.test :refer [deftest is testing]]))

(deftest flux-policy-leave-tests
  (testing "Flux policy continue" 
    (let [entry (create-log-entry :leave-cluster {:id :c})
          f (partial extensions/apply-log-entry entry)
          rep-diff (partial extensions/replica-diff entry)
          rep-reactions (partial extensions/reactions entry)
          old-replica (merge replica/base-replica 
                             {:messaging {:onyx.messaging/impl :dummy-messenger}
                              :job-scheduler :onyx.job-scheduler/greedy
                              :jobs [:j1]
                              :tasks {:j1 [:t1 :t2]}
                              :allocations {:j1 {:t1 [:a] :t2 [:b :c]}}
                              :flux-policies {:j1 {:t2 :kill}}
                              :min-required-peers {:j1 {:t1 1 :t2 2}}
                              :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                              :task-saturation {:j1 {:t1 42 :t2 42}}
                              :peer-state {:a :active :b :active :c :active}
                              :peers [:a :b :c]})
          new-replica (f old-replica)]
      (is (= (:killed-jobs new-replica) [:j1]))
      (is (= (get-in new-replica [:killed-jobs]) [:j1]))))

  (testing "Flux policy kill" 
    (let [entry (create-log-entry :leave-cluster {:id :c})
          f (partial extensions/apply-log-entry entry)
          old-replica (merge replica/base-replica 
                             {:messaging {:onyx.messaging/impl :dummy-messenger}
                              :job-scheduler :onyx.job-scheduler/greedy
                              :jobs [:j1]
                              :tasks {:j1 [:t1 :t2]}
                              :allocations {:j1 {:t1 [:a] :t2 [:b :c]}}
                              :flux-policies {:j1 {:t2 :continue}}
                              :min-required-peers {:j1 {:t1 1 :t2 2}}
                              :task-schedulers {:j1 :onyx.task-scheduler/balanced}
                              :task-saturation {:j1 {:t1 42 :t2 42}}
                              :peer-state {:a :active :b :active :c :active}
                              :peers [:a :b :c]})
          new-replica (f old-replica)]
      (is (= (:killed-jobs new-replica) [:j1]))
      (is (= (get-in new-replica [:killed-jobs]) [:j1])))))
