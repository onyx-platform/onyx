(ns onyx.log.prepare-join-cluster-test
  (:require [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [com.stuartsierra.component :as component]
            [onyx.log.replica :as replica]
            [onyx.system]
            [schema.test] 
            [onyx.peer.log-version]
            [clojure.test :refer [deftest is testing use-fixtures]]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-prepare-join-cluster
  (let [entry (create-log-entry :prepare-join-cluster {:joiner :d
                                                       :peer-site {:address 1}})
        f (partial extensions/apply-log-entry (assoc entry :message-id 0))
        rep-diff (partial extensions/replica-diff entry)
        rep-reactions (partial extensions/reactions entry)
        old-replica (merge replica/base-replica
                           {:messaging {:onyx.messaging/impl :aeron}
                            :pairs {:a :b :b :c :c :a} :groups [:a :b :c]
                            :peer-sites {}
                            :log-version onyx.peer.log-version/version
                            :job-scheduler :onyx.job-scheduler/balanced})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id :a :type :group})]
    (is (= {:a :d} (:prepared new-replica)))
    (is (= {:observer :a :subject :d} diff))
    (is (= [{:fn :notify-join-cluster
             :args {:observer :d}}] 
           reactions))

    (let [old-replica (assoc-in old-replica [:prepared :a] :e)
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :b :type :group})]
      (is (= {:a :e :b :d} (:prepared new-replica)))
      (is (= {:observer :b :subject :d} diff))
      (is (= [{:fn :notify-join-cluster
               :args {:observer :d}}] 
             reactions)))
    (let [old-replica (-> old-replica
                          (assoc-in [:prepared :a] :e)
                          (assoc-in [:prepared :b] :f)
                          (assoc-in [:prepared :c] :g))
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :d :type :group})]
      (is (= {:a :e :b :f :c :g} (:prepared new-replica)))
      (is (= nil diff))
      (is (= [{:fn :abort-join-cluster
               :args {:id :d}}]
             reactions)))

    (let [old-replica (merge replica/base-replica 
                             {:messaging {:onyx.messaging/impl :aeron}
                              :groups []
                              :log-version onyx.peer.log-version/version
                              :job-scheduler :onyx.job-scheduler/balanced})
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :d :type :group})]
      (is (= [:d] (:groups new-replica)))
      (is (= {:instant-join :d} diff))
      (is (= nil reactions)))

    (let [old-replica (merge replica/base-replica
                             {:messaging {:onyx.messaging/impl :aeron}
                              :job-scheduler :onyx.job-scheduler/greedy
                              :log-version onyx.peer.log-version/version
                              :groups [:a]})
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :a :type :group})]
      (is (= [:a] (:groups new-replica)))
      (is (= {:a :d} (:prepared new-replica)))
      (is (= {:observer :a :subject :d} diff))
      (is (= [{:fn :notify-join-cluster
               :args {:observer :d}}] 
             reactions)))

    (let [old-replica (merge replica/base-replica
                             {:messaging {:onyx.messaging/impl :aeron}
                              :pairs {:a :b :b :a}
                              :accepted {}
                              :prepared {:a :c}
                              :groups [:a :b]
                              :aborted #{:d}
                              :log-version onyx.peer.log-version/version
                              :job-scheduler :onyx.job-scheduler/balanced})
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id :d :type :group})]
      (is (= old-replica new-replica))
      (is (= nil diff))
      (is (= [{:fn :abort-join-cluster
               :args {:id :d}}]
             reactions)))))
