(ns onyx.log.notify-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger]
            [onyx.log.replica :as replica]
            [onyx.test-helper :refer [load-config]]
            [onyx.extensions :as extensions]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.api]
            [schema.test]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.log.curator :as zk]))

(use-fixtures :once schema.test/validate-schemas)

(deftest log-notify-test
  (let [onyx-id (java.util.UUID/randomUUID)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/id onyx-id)
        env (onyx.api/start-env env-config)
        _ (extensions/write-chunk (:log env) :job-scheduler {:job-scheduler :onyx.job-scheduler/greedy} nil)
        _ (extensions/write-chunk (:log env) :messaging {:onyx.messaging/impl :dummy-messaging} nil)
        a-id :a
        b-id :b
        c-id :c
        d-id :d
        _ (extensions/register-pulse (:log env) a-id)
        _ (extensions/register-pulse (:log env) b-id)
        _ (extensions/register-pulse (:log env) c-id)
        _ (extensions/register-pulse (:log env) d-id)
        entry (create-log-entry :prepare-join-cluster {:joiner d-id
                                                       :peer-site {:address 1}})
        ch (chan 5)
        _ (extensions/write-log-entry (:log env) entry)
        _ (extensions/subscribe-to-log (:log env) ch)
        read-entry (<!! ch)
        f (partial extensions/apply-log-entry read-entry)
        rep-diff (partial extensions/replica-diff read-entry)
        rep-reactions (partial extensions/reactions read-entry)
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :dummy-messenger}
                            :pairs {a-id b-id b-id c-id c-id a-id} :peers [a-id b-id c-id]
                            :job-scheduler :onyx.job-scheduler/greedy})
        old-local-state {:messenger :dummy-messenger
                         :log (:log env) :id a-id
                         :monitoring (no-op-monitoring-agent)}
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id a-id})
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
                           :monitoring (no-op-monitoring-agent)}
          new-replica (f old-replica)
          diff (rep-diff old-replica new-replica)
          reactions (rep-reactions old-replica new-replica diff {:id d-id})

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
          (is (= :leave-cluster (:fn entry)))
          (is (= {:id :d} (:args entry))))

        (onyx.api/shutdown-env env)))))
