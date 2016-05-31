(ns onyx.log.group-leave-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [onyx.system]
            [onyx.log.replica :as replica]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-leave-cluster-1
  (let [entry (create-log-entry :group-leave-cluster {:id :c})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:pairs {:a :b :b :c :c :a} :groups [:a :b :c]
                            :messaging {:onyx.messaging/impl :dummy-messenger}
                            :job-scheduler :onyx.job-scheduler/greedy})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= :b (get-in new-replica [:pairs :a])))
    (is (= :a (get-in new-replica [:pairs :b])))
    (is (= nil (get-in new-replica [:pairs :c])))
    (is (= {:died :c :updated-watch {:observer :b :subject :a}} diff))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :a})))))

(deftest log-leave-cluster-2
  (let [entry (create-log-entry :group-leave-cluster {:id :b})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:pairs {:a :b :b :a} :groups [:a :b]
                            :messaging {:onyx.messaging/impl :dummy-messenger}
                            :job-scheduler :onyx.job-scheduler/greedy})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= nil (get-in new-replica [:pairs :a])))
    (is (= nil (get-in new-replica [:pairs :b])))
    (is (= {:died :b :updated-watch {:observer :a :subject :a}} diff))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :a})))))

(deftest log-leave-cluster-3 
  (let [entry (create-log-entry :group-leave-cluster {:id :d})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:job-scheduler :onyx.job-scheduler/balanced
                            :pairs {:a :b :b :c :c :d :d :a}
                            :groups [:a :b :c :d]
                            :peers [:a-peer :b-peer :c-peer :d-peer]
                            :peer-state {:a-peer :idle
                                         :b-peer :idle
                                         :c-peer :idle
                                         :d-peer :idle}
                            :groups-index {:a #{:a-peer}
                                           :b #{:b-peer}
                                           :c #{:c-peer}
                                           :d #{:d-peer}}
                            :groups-reverse-index {:a-peer :a
                                                   :b-peer :b
                                                   :c-peer :c
                                                   :d-peer :d}
                            :jobs [:j1 :j2]
                            :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                              :j2 :onyx.task-scheduler/balanced}
                            :task-slot-ids {:j1 {:t1 {:a-peer 1 :b-peer 0}}
                                            :j2 {:t2 {:c-peer 0 :d-peer 1}}}
                            :tasks {:j1 [:t1] :j2 [:t2]}
                            :allocations {:j1 {:t1 [:a-peer :b-peer]}
                                          :j2 {:t2 [:c-peer :d-peer]}}
                            :messaging {:onyx.messaging/impl :dummy-messenger}})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= {:j1 {:t1 [:a-peer :b-peer]} :j2 {:t2 [:c-peer]}} (:allocations (f old-replica))))
    (is (= {:j1 {:t1 {:a-peer 1 :b-peer 0}} :j2 {:t2 {:c-peer 0}}} (:task-slot-ids new-replica)))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :a-peer})))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :b-peer})))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :c-peer})))))

(deftest log-leave-cluster-4 
  (let [entry (create-log-entry :group-leave-cluster {:id :c})
        f (partial extensions/apply-log-entry entry)
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica 
                           {:job-scheduler :onyx.job-scheduler/balanced
                            :messaging {:onyx.messaging/impl :dummy-messenger}
                            :pairs {:a :b :b :c :c :a}
                            :groups [:a :b :c]
                            :peers [:a-peer :b-peer :c-peer]
                            :groups-index {:a #{:a-peer}
                                           :b #{:b-peer}
                                           :c #{:c-peer}}
                            :groups-reverse-index {:a-peer :a
                                                   :b-peer :b
                                                   :c-peer :c}
                            :peer-state {:a-peer :idle
                                         :b-peer :idle
                                         :c-peer :idle}
                            :jobs [:j1 :j2]
                            :task-schedulers {:j1 :onyx.task-scheduler/balanced
                                              :j2 :onyx.task-scheduler/balanced}
                            :tasks {:j1 [:t1] :j2 [:t2]}
                            :task-slot-ids {:j1 {:t1 {:a-peer 1 :b-peer 0}}
                                            :j2 {:t2 {:c-peer 0}}}
                            :allocations {:j1 {:t1 [:a-peer :b-peer]} :j2 {:t2 [:c-peer]}}})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= {:j1 {:t1 [:a-peer]} :j2 {:t2 [:b-peer]}} (:allocations new-replica)))
    (is (= {:j1 {:t1 {:a-peer 1}} :j2 {:t2 {:b-peer 0}}} (:task-slot-ids new-replica)))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :a-peer})))
    (is (= nil (rep-reactions old-replica new-replica diff {:id :b-peer})))))
