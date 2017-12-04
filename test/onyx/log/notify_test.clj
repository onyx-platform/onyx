(ns onyx.log.notify-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.log.replica :as replica]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.extensions :as extensions]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.api]
            [schema.test]
            [onyx.static.uuid :refer [random-uuid]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.log.curator :as zk]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-notify-test
  (let [onyx-id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        env (onyx.api/start-env env-config)
        _ (extensions/write-chunk (:log env) :log-parameters {:onyx.messaging/impl :aeron
                                                              :log-version onyx.peer.log-version/version
                                                              :job-scheduler :onyx.job-scheduler/greedy} nil)
        a-id :a
        b-id :b
        c-id :c
        d-id :d
        _ (extensions/register-pulse (:log env) a-id)
        _ (extensions/register-pulse (:log env) b-id)
        _ (extensions/register-pulse (:log env) c-id)
        _ (extensions/register-pulse (:log env) d-id)
        entry (create-log-entry :prepare-join-cluster {:joiner d-id})
        ch (chan 5)
        _ (extensions/write-log-entry (:log env) entry)
        _ (extensions/subscribe-to-log (:log env) ch)
        read-entry (<!! ch)
        f (partial extensions/apply-log-entry read-entry)
        rep-diff (partial extensions/replica-diff read-entry)
        rep-reactions (partial extensions/reactions read-entry)
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :aeron}
                            :log-version onyx.peer.log-version/version
                            :pairs {a-id b-id b-id c-id c-id a-id} :groups [a-id b-id c-id]
                            :job-scheduler :onyx.job-scheduler/greedy})
        old-local-state {:messenger :aeron
                         :log (:log env) 
                         :id a-id
                         :type :group
                         :monitoring (no-op-monitoring-agent)}
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff old-local-state)
        _ (doseq [reaction reactions]
            (let [log-entry (create-log-entry (:fn reaction) (:args reaction))]
              (extensions/write-log-entry (:log env) log-entry)))
        new-local-state (extensions/fire-side-effects! 
                          read-entry old-replica new-replica diff old-local-state)
        read-entry (<!! ch)]

    (testing "Log notify step 1"
      (is :notify-join-cluster (:fn read-entry))
      (is {:observer d-id :subject b-id} (:args read-entry)))

    (let [f (partial extensions/apply-log-entry read-entry)
          rep-diff (partial extensions/replica-diff read-entry)
          rep-reactions (partial extensions/reactions read-entry)
          old-replica new-replica
          old-local-state {:log (:log env) :id d-id :watch-ch (chan)
                           :type :group
                           :monitoring (no-op-monitoring-agent)}
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff old-local-state)
          _ (doseq [reaction reactions]
              (let [log-entry (create-log-entry (:fn reaction) (:args reaction))]
                (extensions/write-log-entry (:log env) log-entry)))
          new-local-state (extensions/fire-side-effects! 
                            read-entry old-replica new-replica diff old-local-state)
          read-entry (<!! ch)]

      (testing "Log notify step 2"
        (is (= :accept-join-cluster (:fn read-entry)))
        (is (= {:accepted-joiner :d
                :accepted-observer :a
                :subject :b
                :observer :d} 
               (:args read-entry))))
      (let [conn (zk/connect (:zookeeper/address (:env-config config)))
            _ (zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" d-id))
            _ (zk/close conn)
            entry (<!! ch)]

        (testing "Log notify step 3" 
          (is (= :group-leave-cluster (:fn entry)))
          (is (= {:id :d} (:args entry))))

        (onyx.api/shutdown-env env)))))
