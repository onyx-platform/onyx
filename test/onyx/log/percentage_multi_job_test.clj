(ns onyx.log.percentage-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
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
   :onyx/id onyx-id
   :onyx.coordinator/revoke-delay 5000})

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
   :onyx.peer/job-scheduler :onyx.job-scheduler/percentage
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
                        :percentage 70
                        :task-scheduler :onyx.task-scheduler/greedy}))

(def j2
  (onyx.api/submit-job peer-config
                       {:workflow [[:c :d]]
                        :catalog catalog-2
                        :percentage 30
                        :task-scheduler :onyx.task-scheduler/greedy}))

(def n-peers 10)

(def v-peers-1 (onyx.api/start-peers! n-peers peer-config system/onyx-fake-peer))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (apply concat (vals (get (:allocations new-replica) j1)))) 7)
                   (= (count (apply concat (vals (get (:allocations new-replica) j2)))) 3))
        (recur new-replica)
        new-replica))))

(fact "70/30% split for percentage job scheduler succeeded" true => true)

(def v-peers-2 (onyx.api/start-peers! n-peers peer-config system/onyx-fake-peer))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (apply concat (vals (get (:allocations new-replica) j1)))) 14)
                   (= (count (apply concat (vals (get (:allocations new-replica) j2)))) 6))
        (recur new-replica)
        new-replica))))

(fact "70/30% split for percentage job scheduler succeeded after rebalance" true => true)

(doseq [v-peer v-peers-1]
  (onyx.api/shutdown-peer v-peer))

(doseq [v-peer v-peers-2]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

