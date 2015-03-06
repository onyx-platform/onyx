(ns onyx.log.round-robin-multi-job-test
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
    :onyx/batch-size 20}

   {:onyx/name :b
    :onyx/fn :onyx.peer.single-peer-test/my-inc
    :onyx/type :function
    :onyx/batch-size 20}

   {:onyx/name :c
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/batch-size 20}])

(def catalog-2
  [{:onyx/name :d
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/batch-size 20}

   {:onyx/name :e
    :onyx/fn :onyx.peer.single-peer-test/my-inc
    :onyx/type :function
    :onyx/batch-size 20}

   {:onyx/name :f
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/batch-size 20}])

(def j1
  (onyx.api/submit-job peer-config
                       {:workflow [[:a :b] [:b :c]]
                        :catalog catalog-1
                        :task-scheduler :onyx.task-scheduler/round-robin}))

(def j2
  (onyx.api/submit-job peer-config
                       {:workflow [[:d :e] [:e :f]]
                        :catalog catalog-2
                        :task-scheduler :onyx.task-scheduler/round-robin}))

(def n-peers 36)

(def v-peers (onyx.api/start-peers n-peers peer-config))

(def ch (chan n-peers))

(def replica-1
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          task-a (nth (get-in new-replica [:tasks j1]) 0)
          task-b (nth (get-in new-replica [:tasks j1]) 1)
          task-c (nth (get-in new-replica [:tasks j1]) 2)
          task-d (nth (get-in new-replica [:tasks j2]) 0)
          task-e (nth (get-in new-replica [:tasks j2]) 1)
          task-f (nth (get-in new-replica [:tasks j2]) 2)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 6)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 6)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 6)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 6)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 6)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 6))
        (recur new-replica)
        new-replica))))

(def task-a (nth (get-in replica-1 [:tasks j1]) 0))

(def task-b (nth (get-in replica-1 [:tasks j1]) 1))

(def task-c (nth (get-in replica-1 [:tasks j1]) 2))

(def task-d (nth (get-in replica-1 [:tasks j2]) 0))

(def task-e (nth (get-in replica-1 [:tasks j2]) 1))

(def task-f (nth (get-in replica-1 [:tasks j2]) 2))

(fact "6 peers are assigned to each task" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task task-d}))

(extensions/write-log-entry (:log env) entry)

(def replica-2
  (loop [replica replica-1]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 6)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 6)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 6)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 9)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 9))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 2, task D completes" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task task-e}))

(extensions/write-log-entry (:log env) entry)

(def replica-3
  (loop [replica replica-2]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 6)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 6)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 6)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 18))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 2, task E completes" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task task-a}))

(extensions/write-log-entry (:log env) entry)

(def replica-4
  (loop [replica replica-3]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 0)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 9)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 9)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 18))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 1, task A completes" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task task-f}))

(extensions/write-log-entry (:log env) entry)

(def replica-5
  (loop [replica replica-4]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 0)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 18)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 18)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 0))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 2, task F completes" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task task-b}))

(extensions/write-log-entry (:log env) entry)

(def replica-6
  (loop [replica replica-5]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 0)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 0)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 36)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 0))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 1, task B completes" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task task-c}))

(extensions/write-log-entry (:log env) entry)

(def replica-7
  (loop [replica replica-6]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (get (get (:allocations new-replica) j1) task-a)) 0)
                   (= (count (get (get (:allocations new-replica) j1) task-b)) 0)
                   (= (count (get (get (:allocations new-replica) j1) task-c)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-d)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-e)) 0)
                   (= (count (get (get (:allocations new-replica) j2) task-f)) 0))
        (recur new-replica)
        new-replica))))

(fact "The peers stop working after job 1, task C completes" true => true)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

