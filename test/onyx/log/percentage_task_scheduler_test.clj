(ns onyx.log.percentage-task-scheduler-test
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
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def n-peers 10)

(def v-peers (onyx.api/start-peers n-peers peer-config))

(def catalog
  [{:onyx/name :a
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/percentage 50
    :onyx/batch-size 20}

   {:onyx/name :b
    :onyx/fn :onyx.peer.percentage-task-scheduler/fn
    :onyx/type :function
    :onyx/percentage 30
    :onyx/batch-size 20}

   {:onyx/name :c
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/percentage 20
    :onyx/batch-size 20}])

(def job-id
  (onyx.api/submit-job
   peer-config
   {:workflow [[:a :b] [:b :c]]
    :catalog catalog
    :task-scheduler :onyx.task-scheduler/percentage}))

(def ch (chan n-peers))

(def replica
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          counts (map count (vals (get-in new-replica [:allocations job-id])))]
      (if-not (= #{2 3 5} (into #{} counts))
        (recur new-replica)
        new-replica))))

(fact "peers balanced in 50/30/20 split" true => true)

(def inversions (clojure.set/map-invert (get-in replica [:task-percentages job-id])))

(def task-a (get inversions 50))

(def task-b (get inversions 30))

(def task-c (get inversions 20))

(def entry (create-log-entry :complete-task {:job job-id :task task-a}))

(extensions/write-log-entry (:log env) entry)

(def entry (create-log-entry :complete-task {:job job-id :task task-b}))

(extensions/write-log-entry (:log env) entry)

(def ch (chan n-peers))

(def replica-2
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          counts (map count (vals (get-in new-replica [:allocations job-id])))]
      (if-not (= #{10} (into #{} counts))
        (recur new-replica)
        new-replica))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

