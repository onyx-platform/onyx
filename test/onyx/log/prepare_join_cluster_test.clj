(ns onyx.log.prepare-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger :refer [dummy-messenger]]
            [com.stuartsierra.component :as component]
            [onyx.log.replica :as replica]
            [onyx.messaging.dummy-messenger]
            [onyx.system]
            [schema.test] 
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-prepare-join-cluster
  (let [entry (create-log-entry :prepare-join-cluster {:joiner :d
                                                       :peer-site {:address 1}})
        f (partial extensions/apply-log-entry (assoc entry :message-id 0))
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :pairs {:a :b :b :c :c :a} :peers [:a :b :c]
                            :peer-sites {}
                            :job-scheduler :onyx.job-scheduler/balanced})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id :a :messenger (dummy-messenger {})})]
    (is (= {:a :d} (:prepared new-replica)))
    (is (= {:observer :a :subject :d} diff))
    (is (= [{:fn :notify-join-cluster
             :args {:observer :d :subject :b}}] 
           reactions))

    (let [old-replica (assoc-in old-replica [:prepared :a] :e)
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :b :messenger (dummy-messenger {})})]
      (is (= {:a :e :b :d} (:prepared new-replica)))
      (is (= {:observer :b :subject :d} diff))
      (is (= [{:fn :notify-join-cluster
                         :args {:observer :d :subject :c}}] 
             reactions)))
    (let [old-replica (-> old-replica
                          (assoc-in [:prepared :a] :e)
                          (assoc-in [:prepared :b] :f)
                          (assoc-in [:prepared :c] :g))
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
      (is (= {:a :e :b :f :c :g} (:prepared new-replica)))
      (is (= nil diff))
      (is (= [{:fn :abort-join-cluster
               :args {:id :d}}] 
             reactions)))

    (let [old-replica (merge replica/base-replica 
                             {:messaging {:onyx.messaging/impl :dummy-messenger}
                              :peers []
                              :job-scheduler :onyx.job-scheduler/balanced})
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
      (is (= [:d] (:peers new-replica)))
      (is (= {:d :idle} (:peer-state new-replica)))
      (is (= {:instant-join :d} diff))
      (is (= nil reactions)))

    (let [old-replica (merge replica/base-replica
                             {:messaging {:onyx.messaging/impl :dummy-messenger}
                              :job-scheduler :onyx.job-scheduler/greedy
                              :peers [:a]})
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :a :messenger (dummy-messenger {})})]
      (is (= [:a] (:peers new-replica)))
      (is (= {:a :d} (:prepared new-replica)))
      (is (= {:observer :a :subject :d} diff))
      (is (= [{:fn :notify-join-cluster
               :args {:observer :d :subject :a}}] 
             reactions)))

    (let [old-replica (merge replica/base-replica
                             {:messaging {:onyx.messaging/impl :dummy-messenger}
                              :pairs {:a :b :b :a}
                              :accepted {}
                              :prepared {:a :c}
                              :peers [:a :b]
                              :job-scheduler :onyx.job-scheduler/balanced})
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :d :messenger (dummy-messenger {})})]
      (is (= old-replica new-replica))
      (is (= nil diff))
      (is (= [{:fn :abort-join-cluster
               :args {:id :d}}] 
             reactions)))))
