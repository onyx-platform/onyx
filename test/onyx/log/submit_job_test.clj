(ns onyx.log.submit-job-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.messaging.dummy-messenger]
            [onyx.system]
            [onyx.api]))

(deftest submit-job-log-test
  (let [entry (create-log-entry :submit-job {:id :a :tasks [:t1]
                                             :task-scheduler :onyx.task-scheduler/balanced
                                             :saturation 42})
        f (partial extensions/apply-log-entry (assoc entry :message-id 0))
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :job-scheduler :onyx.job-scheduler/greedy
                            :peers [:p1]})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id :x})]

    (is (= [:a] (:jobs new-replica)))
    (is (= {:job :a} diff)))
    
  (let [entry (create-log-entry :submit-job {:id :a :tasks [:t1]
                                             :task-scheduler :onyx.task-scheduler/balanced
                                             :saturation 42})
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :job-scheduler :onyx.job-scheduler/greedy
                            :jobs [:b]
                            :task-schedulers {:b :onyx.task-scheduler/balanced}
                            :tasks {:b [:t1 :t2]}})
        f (partial extensions/apply-log-entry (assoc entry :message-id 0))
        rep-diff (partial extensions/replica-diff entry)
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        rep-reactions (partial extensions/reactions entry)
        reactions (rep-reactions old-replica new-replica diff {:id :x})]
    (is (= [:b :a] (:jobs new-replica)))
    (is (= {:job :a} diff)))

  (let [entry (create-log-entry :submit-job {:id :j1
                                             :task-scheduler :onyx.task-scheduler/balanced
                                             :saturation 42
                                             :tasks [:t1 :t2 :t3]})
        f (partial extensions/apply-log-entry (assoc entry :message-id 0))
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :job-scheduler :onyx.job-scheduler/balanced
                            :peers [:p1 :p2]
                            :peer-state {:p1 :idle :p2 :idle}})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= [] (rep-reactions old-replica new-replica diff {:id :p1})))
    (is (= [] (rep-reactions old-replica new-replica diff {:id :p2})))))
