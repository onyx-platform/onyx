(ns onyx.log.peer-rebalance-test
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

(def scheduler :onyx.job-scheduler/round-robin)

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
   :onyx.peer/job-scheduler scheduler
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
   :onyx.peer/job-scheduler scheduler
   :onyx.peer/state {:task-lifecycle-fn util/stub-task-lifecycle}})

(def env (onyx.api/start-env env-config))

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :onyx/batch-size 20}

   {:onyx/name :b
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :onyx/batch-size 20}])

(def catalog-2
  [{:onyx/name :c
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :onyx/batch-size 20}

   {:onyx/name :d
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :onyx/batch-size 20}])

(def j1
  (onyx.api/submit-job peer-config
                       {:workflow [[:a :b]]
                        :catalog catalog-1
                        :task-scheduler :onyx.task-scheduler/round-robin}))

(def j2
  (onyx.api/submit-job peer-config
                       {:workflow [[:c :d]]
                        :catalog catalog-2
                        :task-scheduler :onyx.task-scheduler/round-robin}))

(def n-peers 12)

(def v-peers (onyx.api/start-peers! n-peers peer-config system/onyx-fake-peer))

(def ch (chan n-peers))

(def replica-1
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          task-a (first (get-in new-replica [:tasks j1]))
          task-b (second (get-in new-replica [:tasks j1]))
          task-c (first (get-in new-replica [:tasks j2]))
          task-d (second (get-in new-replica [:tasks j2]))]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 3)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 3)
                   (= (count (get (get (:allocations new-replica) j2) task-c)) 3)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 3))
        (recur new-replica)
        new-replica))))

(def task-a (first (get-in replica-1 [:tasks j1])))

(def task-b (second (get-in replica-1 [:tasks j1])))

(def task-c (first (get-in replica-1 [:tasks j2])))

(def task-d (second (get-in replica-1 [:tasks j2])))

(fact "the peers evenly balance" true => true)

(def conn (zk/connect (:address (:zookeeper config))))

(def id (last (get (get (:allocations replica-1) j1) task-b)))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" id))

(zk/close conn)

(def replica-2
  (loop [replica replica-1]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 3)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 3)
                   (= (count (get (get (:allocations new-replica) j2) task-c)) 3)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 2))
        (recur new-replica)
        new-replica))))

(fact "the peers rebalance" true => true)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

