(ns onyx.log.pulse-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(def a-id "a")

(def b-id "b")

(def c-id "c")

(def d-id "d")

(def entry (create-log-entry :prepare-join-cluster {:joiner d-id}))

(def ch (chan 5))

(extensions/write-log-entry (:log env) entry)

(extensions/subscribe-to-log (:log env) 0 ch)

(def read-entry (extensions/read-log-entry (:log env) (<!! ch)))

(def f (partial extensions/apply-log-entry read-entry))

(def rep-diff (partial extensions/replica-diff read-entry))

(def rep-reactions (partial extensions/reactions read-entry))

(extensions/register-pulse (:log env) a-id)
(extensions/register-pulse (:log env) b-id)
(extensions/register-pulse (:log env) c-id)
(extensions/register-pulse (:log env) d-id)

(def old-replica {:pairs {a-id b-id b-id c-id c-id a-id} :peers [a-id b-id c-id]})

(def new-replica (f old-replica))

(def diff (rep-diff old-replica new-replica))

(def reactions (rep-reactions old-replica new-replica diff {:id d-id}))

(extensions/fire-side-effects! read-entry old-replica new-replica diff {:log (:log env) :id a-id})

(def conn (zk/connect (:zookeeper/address (:zookeeper (:env config)))))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" d-id))

(zk/close conn)

(def entry (extensions/read-log-entry (:log env) (<!! ch)))

(fact (:fn entry) => :leave-cluster)
(fact (:args entry) => {:id "d"})

(component/stop env)

