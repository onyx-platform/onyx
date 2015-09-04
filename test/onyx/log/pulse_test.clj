(ns onyx.log.pulse-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.messaging.dummy-messenger]
            [onyx.monitoring.no-op-monitoring :refer [no-op-monitoring-agent]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.log.replica :as replica]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [onyx.log.curator :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def env (onyx.api/start-env env-config))

(extensions/write-chunk (:log env) :job-scheduler {:job-scheduler :onyx.job-scheduler/balanced} nil)
(extensions/write-chunk (:log env) :messaging {:onyx.messaging/impl :dummy-messenger} nil)

(def a-id :a)

(def b-id :b)

(def c-id :c)

(def d-id :d)

(def entry (create-log-entry :prepare-join-cluster {:joiner d-id
                                                    :peer-site {:address 1}}))

(def ch (chan 5))

(extensions/write-log-entry (:log env) entry)

(extensions/subscribe-to-log (:log env) ch)

(def read-entry (<!! ch))

(def f (partial extensions/apply-log-entry read-entry))

(def rep-diff (partial extensions/replica-diff read-entry))

(def rep-reactions (partial extensions/reactions read-entry))

(extensions/register-pulse (:log env) a-id)
(extensions/register-pulse (:log env) b-id)
(extensions/register-pulse (:log env) c-id)
(extensions/register-pulse (:log env) d-id)

(def old-replica (merge replica/base-replica 
                        {:messaging {:onyx.messaging/impl :dummy-messenger}
                         :job-scheduler :onyx.job-scheduler/greedy
                         :pairs {a-id b-id b-id c-id c-id a-id} :peers [a-id b-id c-id]}))

(def new-replica (f old-replica))

(def diff (rep-diff old-replica new-replica))

(def reactions (rep-reactions old-replica new-replica diff {:id d-id}))

(def state {:log (:log env) :id a-id
            :monitoring (no-op-monitoring-agent)})

(extensions/fire-side-effects! read-entry old-replica new-replica diff state)

(def conn (zk/connect (:zookeeper/address (:env-config config))))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" d-id))

(zk/close conn)

(def entry (<!! ch))

(fact (:fn entry) => :leave-cluster)
(fact (:args entry) => {:id :d})

(onyx.api/shutdown-env env)
