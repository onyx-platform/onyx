(ns onyx.log.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
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
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
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
  (onyx.api/submit-job
   peer-config
   {:workflow [[:a :b]]
    :catalog catalog-1
    :task-scheduler :onyx.task-scheduler/greedy}))

(def j2
  (onyx.api/submit-job
   peer-config
   {:workflow [[:c :d]]
    :catalog catalog-2
    :task-scheduler :onyx.task-scheduler/greedy}))

(def n-peers 40)

(def v-peers (onyx.api/start-peers! n-peers peer-config))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          task-a (first (get-in new-replica [:tasks j1]))
          task-b (second (get-in new-replica [:tasks j1]))
          task-c (first (get-in new-replica [:tasks j2]))
          task-d (second (get-in new-replica [:tasks j2]))]
      (if-not (and (= (count (get-in new-replica [:allocations j1 task-a])) 40)
                   (zero? (apply + (map count (vals (get (:allocations new-replica) j2))))))
        (recur new-replica)
        new-replica))))

(fact "40 peers were allocated to job 1, task A" true => true)

(def task-a (first (get-in replica [:tasks j1])))

(def task-b (second (get-in replica [:tasks j1])))

(def task-c (first (get-in replica [:tasks j2])))

(def task-d (second (get-in replica [:tasks j2])))

(def entry (create-log-entry :complete-task {:job j1 :task task-a}))

(extensions/write-log-entry (:log env) entry)

(def replica-2
  (loop [replica replica]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (and (= (count (get (get (:allocations new-replica) j1) task-a)) 40)
               (zero? (apply + (map count (vals (get (:allocations new-replica) j2))))))
        new-replica
        (recur new-replica)))))

(fact "All peers were reallocated to job 1, task B" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task task-b}))

(extensions/write-log-entry (:log env) entry)

(def replica-3
  (loop [replica replica-2]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (= (count (get (get (:allocations new-replica) j2) task-c)) 40)
        new-replica
        (recur new-replica)))))

(fact "All peers were reallocated to job 2, task C" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task task-c}))

(extensions/write-log-entry (:log env) entry)

(def replica-4
  (loop [replica replica-3]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (= (count (get (get (:allocations new-replica) j2) task-d)) 40)
        new-replica
        (recur new-replica)))))

(fact "All peers were reallocated to job 2, task D" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task task-d}))

(extensions/write-log-entry (:log env) entry)

(def replica-5
  (loop [replica replica-4]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (zero? (apply + (map count (vals (get (:allocations new-replica) j2)))))
        new-replica
        (recur new-replica)))))

(fact "No peers are executing any tasks" true => true)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

