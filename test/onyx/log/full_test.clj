(ns onyx.log.full-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(def peer-opts
  {:inbox-capacity 100
   :outbox-capacity 100})

(def v-peers (onyx.api/start-peers! onyx-id 20 (:peer config) peer-opts))

(doseq [v-peer v-peers]
  (try
    (prn "go")
    ((:shutdown-fn v-peer))
    (prn "Done")
    (catch Exception e (prn e))))

(component/stop env)

