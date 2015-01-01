(ns onyx.log.percentage-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
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

(def v-peers (onyx.api/start-peers! n-peers peer-config))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 7)
                   (= (count (:c (get (:allocations new-replica) j2))) 3))
        (recur new-replica)
        new-replica))))

(fact "70/30% split for percentage job scheduler succeeded" true => true)

(comment
  (def entry (create-log-entry :complete-task {:job j1 :task :a}))

  (extensions/write-log-entry (:log env) entry)

  (def replica-2
    (loop [replica replica]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log env) position)
            new-replica (extensions/apply-log-entry entry replica)]
        (if (and (= (count (:b (get (:allocations new-replica) j1))) 40)
                 (zero? (apply + (map count (vals (get (:allocations new-replica) j2))))))
          new-replica
          (recur new-replica)))))

  (fact "All peers were reallocated to job 1, task B" true => true)

  (def entry (create-log-entry :complete-task {:job j1 :task :b}))

  (extensions/write-log-entry (:log env) entry)

  (def replica-3
    (loop [replica replica-2]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log env) position)
            new-replica (extensions/apply-log-entry entry replica)]
        (if (= (count (:c (get (:allocations new-replica) j2))) 40)
          new-replica
          (recur new-replica)))))

  (fact "All peers were reallocated to job 2, task C" true => true)

  (def entry (create-log-entry :complete-task {:job j2 :task :c}))

  (extensions/write-log-entry (:log env) entry)

  (def replica-4
    (loop [replica replica-3]
      (let [position (<!! ch)
            entry (extensions/read-log-entry (:log env) position)
            new-replica (extensions/apply-log-entry entry replica)]
        (if (= (count (:d (get (:allocations new-replica) j2))) 40)
          new-replica
          (recur new-replica)))))

  (fact "All peers were reallocated to job 2, task D" true => true)

  (def entry (create-log-entry :complete-task {:job j2 :task :d}))

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

)