(ns onyx.log.gc-log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.log.util :as util]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config
  {:zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id onyx-id})

(def peer-config
  {:zookeeper/address (:address (:zookeeper config))
   :onyx/id onyx-id
   :onyx.messaging/impl :http-kit-websockets
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy})

(def env (onyx.api/start-env env-config))

(def n-peers 5)

(def v-peers (onyx.api/start-peers n-peers peer-config))

(onyx.api/gc peer-config)

(def v-peers (onyx.api/start-peers n-peers peer-config))

(def ch (chan 100))

(loop [replica (extensions/subscribe-to-log (:log env) ch)]
  (let [position (<!! ch)
        entry (extensions/read-log-entry (:log env) position)
        new-replica (extensions/apply-log-entry entry replica)]
    (when-not (= (count (:peers new-replica)) 10)
      (recur new-replica))))

(fact "Starting peers after GC succeeded" true => true)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

