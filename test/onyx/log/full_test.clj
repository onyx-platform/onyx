(ns onyx.log.full-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def scheduler :onyx.job-scheduler/greedy)

(def env-config
  {:zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id onyx-id})

(def peer-config
  {:zookeeper/address (:address (:zookeeper config))
   :onyx/id onyx-id
   :onyx.messaging/impl :http-kit-websockets
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def n-peers 50)

(def v-peers (onyx.api/start-peers n-peers peer-config))

(def ch (chan n-peers))

(def replica
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (< (count (:pairs new-replica)) n-peers)
        (recur new-replica)
        new-replica))))

(fact (:prepared replica) => {})
(fact (:accepted replica) => {})
(fact (count (:peers replica)) => n-peers)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

