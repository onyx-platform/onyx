(ns onyx.log.gc-log-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :as system]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.log.util :as util]
            [onyx.api :as api]
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
   :onyx/id onyx-id})

(def peer-config
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id onyx-id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.peer/state {:task-lifecycle-fn util/stub-task-lifecycle}})

(def env (onyx.api/start-env env-config))

(def n-peers 5)

(def v-peers (onyx.api/start-peers! n-peers peer-config system/onyx-fake-peer))

(onyx.api/gc peer-config)

;;(def zk (zk/connect "127.0.0.1:2185"))

(clojure.pprint/pprint (clojure.data.fressian/read (:data (zk/data zk (str "/onyx/" onyx-id "/origin/origin")))))


;; (def ch (chan n-peers))

;; (extensions/subscribe-to-log (:log env) 0 ch)

;; (def replica
;;   (loop [replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}]
;;     (let [position (<!! ch)
;;           entry (extensions/read-log-entry (:log env) position)
;;           new-replica (extensions/apply-log-entry entry replica)]
;;       (clojure.pprint/pprint (str position "::" entry))
;;       (recur new-replica))))

;; (fact "peers balanced on 1 jobs" true => true)

;; (doseq [v-peer v-peers]
;;   (onyx.api/shutdown-peer v-peer))

;; (onyx.api/shutdown-env env)

