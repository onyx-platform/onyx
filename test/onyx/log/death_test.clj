(ns onyx.log.death-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [midje.sweet :refer :all]
            [zookeeper :as zk]
            [onyx.api]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(comment
  (future
    (def ch (chan 100))

    (extensions/subscribe-to-log (:log env) 0 ch)

    (loop [replica {}]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log env) position)
            new-replica (extensions/apply-log-entry entry replica)]
        (clojure.pprint/pprint entry)
        (clojure.pprint/pprint new-replica)
        (prn "==")
        (recur new-replica))))

  (def peer-opts
    {:inbox-capacity 1000
     :outbox-capacity 1000})

  (def ps (onyx.api/start-peers! onyx-id 3 (:peer config) peer-opts))

  ;;(def p (onyx.api/start-peers! onyx-id 1 (:peer config) peer-opts))

  ((:shutdown-fn (nth ps 0)))
  ((:shutdown-fn (nth ps 1)))
  ((:shutdown-fn (nth ps 2)))

  (component/stop env))

