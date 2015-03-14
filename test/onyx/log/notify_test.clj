(ns onyx.log.notify-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def env (onyx.api/start-env env-config))

(extensions/write-chunk (:log env) :job-scheduler {:job-scheduler :onyx.job-scheduler/greedy} nil)

(def a-id "a")

(def b-id "b")

(def c-id "c")

(def d-id "d")

(extensions/register-pulse (:log env) a-id)
(extensions/register-pulse (:log env) b-id)
(extensions/register-pulse (:log env) c-id)
(extensions/register-pulse (:log env) d-id)

(def entry (create-log-entry :prepare-join-cluster {:joiner d-id}))

(def ch (chan 5))

(extensions/write-log-entry (:log env) entry)

(extensions/subscribe-to-log (:log env) ch)

(def message-id (<!! ch))

(def read-entry (extensions/read-log-entry (:log env) message-id))

(def f (partial extensions/apply-log-entry read-entry))

(def rep-diff (partial extensions/replica-diff read-entry))

(def rep-reactions (partial extensions/reactions read-entry))

(def old-replica {:pairs {a-id b-id b-id c-id c-id a-id} :peers [a-id b-id c-id]})

(def old-local-state {:log (:log env) :id a-id})

(def new-replica (f old-replica))

(def diff (rep-diff old-replica new-replica))

(def reactions (rep-reactions old-replica new-replica diff {:id a-id}))

(doseq [reaction reactions]
  (let [log-entry (create-log-entry (:fn reaction) (:args reaction))]
    (extensions/write-log-entry (:log env) log-entry)))

(def new-local-state
  (extensions/fire-side-effects! read-entry old-replica new-replica diff old-local-state))

(def message-id (<!! ch))

(def read-entry (extensions/read-log-entry (:log env) message-id))

(fact (:fn read-entry) => :notify-join-cluster)
(fact (:args read-entry) => {:observer d-id :subject b-id})

(def f (partial extensions/apply-log-entry read-entry))

(def rep-diff (partial extensions/replica-diff read-entry))

(def rep-reactions (partial extensions/reactions read-entry))

(def old-replica new-replica)

(def old-local-state {:log (:log env) :id d-id :watch-ch (chan)})

(def new-replica (f old-replica))

(def diff (rep-diff old-replica new-replica))

(def reactions (rep-reactions old-replica new-replica diff {:id d-id}))

(doseq [reaction reactions]
  (let [log-entry (create-log-entry (:fn reaction) (:args reaction))]
    (extensions/write-log-entry (:log env) log-entry)))

(def new-local-state
  (extensions/fire-side-effects! read-entry old-replica new-replica diff old-local-state))

(def message-id (<!! ch))

(def read-entry (extensions/read-log-entry (:log env) message-id))

(fact (:fn read-entry) => :accept-join-cluster)
(fact (:args read-entry) => {:accepted-joiner "d"
                             :accepted-observer "a"
                             :subject "b"
                             :observer "d"})

(def conn (zk/connect (:zookeeper/address (:env-config config))))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" d-id))

(zk/close conn)

(def entry (extensions/read-log-entry (:log env) (<!! ch)))

(fact (:fn entry) => :leave-cluster)
(fact (:args entry) => {:id "d"})

(onyx.api/shutdown-env env)


