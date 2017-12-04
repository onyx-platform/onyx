(ns onyx.log.submit-job-test
  (:require [clojure.test :refer :all]
            [schema.test]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.log.generators :refer [one-group]]
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
                           (one-group
                            {:messaging {:onyx.messaging/impl :aeron}
                             :job-scheduler :onyx.job-scheduler/greedy
                             :groups [:g1]
                             :groups-index {:g1 #{:p1}}
                             :peers [:p1]}))
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id :x :type :group})]
    (is (= [:a] (:jobs new-replica)))
    (is (= {:job :a} diff)))
    
  (let [entry (create-log-entry :submit-job {:id :a :tasks [:t1]
                                             :task-scheduler :onyx.task-scheduler/balanced
                                             :saturation 42})
        old-replica (merge replica/base-replica
                           (one-group
                            {:messaging {:onyx.messaging/impl :aeron}
                             :job-scheduler :onyx.job-scheduler/greedy
                             :jobs [:b]
                             :task-schedulers {:b :onyx.task-scheduler/balanced}
                             :tasks {:b [:t1 :t2]}}))
        f (partial extensions/apply-log-entry (assoc entry :message-id 0))
        rep-diff (partial extensions/replica-diff entry)
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        rep-reactions (partial extensions/reactions entry)
        reactions (rep-reactions old-replica new-replica diff {:id :x :type :group})]
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
                           (one-group
                            {:messaging {:onyx.messaging/impl :aeron}
                             :job-scheduler :onyx.job-scheduler/balanced
                             :groups [:g1]
                             :groups-index {:g1 #{:p1 :p2}}
                             :peers [:p1 :p2]}))
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)]
    (is (= [] (rep-reactions old-replica new-replica diff {:id :p1 :type :group})))
    (is (= [] (rep-reactions old-replica new-replica diff {:id :p2 :type :group})))))
