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
  {:inbox-capacity 1000
   :outbox-capacity 1000})

(def n-peers 50)

(def v-peers (onyx.api/start-peers! onyx-id n-peers (:peer config) peer-opts))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {}]
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
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(component/stop env)

