(ns onyx.log.pulse-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.log.replica :as replica]
            [onyx.api :as api]
            [schema.test] 
            [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.log.curator :as zk]))

(deftest log-pulse-test
  (let [onyx-id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id onyx-id)
        env (onyx.api/start-env env-config)
        _ (extensions/write-chunk (:log env) :log-parameters {:onyx.messaging/impl :dummy-messenger
                                                              :log-version onyx.peer.log-version/version
                                                              :job-scheduler :onyx.job-scheduler/balanced} nil)
        a-id :a
        b-id :b
        c-id :c
        d-id :d
        entry (create-log-entry :prepare-join-cluster {:joiner d-id})
        ch (chan 5)
        _ (extensions/write-log-entry (:log env) entry)
        _ (extensions/subscribe-to-log (:log env) ch)
        read-entry (<!! ch)
        f (partial extensions/apply-log-entry read-entry)
        rep-diff (partial extensions/replica-diff read-entry)
        rep-reactions (partial extensions/reactions read-entry)
        _ (extensions/register-pulse (:log env) a-id)
        _ (extensions/register-pulse (:log env) b-id)
        _ (extensions/register-pulse (:log env) c-id)
        _ (extensions/register-pulse (:log env) d-id)
        old-replica (merge replica/base-replica 
                           {:messaging {:onyx.messaging/impl :aeron}
                            :job-scheduler :onyx.job-scheduler/greedy
                            :pairs {a-id b-id b-id c-id c-id a-id} :groups [a-id b-id c-id]})
        new-replica (f old-replica)
        diff (rep-diff old-replica new-replica)
        reactions (rep-reactions old-replica new-replica diff {:id d-id :type :group})
        state {:log (:log env) :id a-id
               :type :group
               :monitoring (no-op-monitoring-agent)}
        _ (extensions/fire-side-effects! read-entry old-replica new-replica diff state)
        conn (zk/connect (:zookeeper/address (:env-config config)))
        _ (zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" d-id))
        _ (zk/close conn)
        entry (<!! ch)]

    (is (= :group-leave-cluster (:fn entry)))
    (is (= {:id :d} (:args entry)))

    (onyx.api/shutdown-env env)))
