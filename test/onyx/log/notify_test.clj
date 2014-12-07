(ns onyx.log.notify-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.log.util :as util]
            [onyx.api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.server/type :embedded
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :hornetq.embedded/config (:configs (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id onyx-id
   :onyx.coordinator/revoke-delay 5000})

(def dev (onyx-development-env env-config))

(def env (component/start dev))

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

(extensions/subscribe-to-log (:log env) 0 ch)

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

(def conn (zk/connect (:address (:zookeeper config))))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" d-id))

(zk/close conn)

(def entry (extensions/read-log-entry (:log env) (<!! ch)))

(fact (:fn entry) => :leave-cluster)
(fact (:args entry) => {:id "d"})

(component/stop env)

