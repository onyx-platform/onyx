(ns onyx.log.round-robin-multi-job-test
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
   :outbox-capacity 1000
   :job-scheduler :onyx.job-scheduler/round-robin})

(def j1
  (onyx.api/submit-job (:log env)
                       {:workflow [[:a :b] [:b :c]]
                        :catalog []
                        :task-scheduler :onyx.task-scheduler/round-robin}))

(def j2
  (onyx.api/submit-job (:log env)
                       {:workflow [[:d :e] [:e :f]]
                        :catalog []
                        :task-scheduler :onyx.task-scheduler/round-robin}))

(def n-peers 36)

(def v-peers (onyx.api/start-peers! onyx-id n-peers (:peer config) peer-opts))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica-1
  (loop [replica {:job-scheduler (:job-scheduler peer-opts)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations replica) j1))) 6)
                   (= (count (:b (get (:allocations replica) j1))) 6)
                   (= (count (:c (get (:allocations replica) j1))) 6)
                   (= (count (:d (get (:allocations replica) j2))) 6)
                   (= (count (:e (get (:allocations replica) j2))) 6)
                   (= (count (:f (get (:allocations replica) j2))) 6))
        (recur new-replica)
        new-replica))))

(fact "6 peers are assigned to each task" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task :d}))

(extensions/write-log-entry (:log env) entry)

(def replica-2
  (loop [replica replica-1]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 6)
                   (= (count (:b (get (:allocations new-replica) j1))) 6)
                   (= (count (:c (get (:allocations new-replica) j1))) 6)
                   (= (count (:d (get (:allocations new-replica) j2))) 0)
                   (= (count (:e (get (:allocations new-replica) j2))) 9)
                   (= (count (:f (get (:allocations new-replica) j2))) 9))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 2, task D completes" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task :e}))

(extensions/write-log-entry (:log env) entry)

(def replica-3
  (loop [replica replica-2]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 6)
                   (= (count (:b (get (:allocations new-replica) j1))) 6)
                   (= (count (:c (get (:allocations new-replica) j1))) 6)
                   (= (count (:d (get (:allocations new-replica) j2))) 0)
                   (= (count (:e (get (:allocations new-replica) j2))) 0)
                   (= (count (:f (get (:allocations new-replica) j2))) 18))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 2, task E completes" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task :a}))

(extensions/write-log-entry (:log env) entry)

(def replica-4
  (loop [replica replica-3]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 0)
                   (= (count (:b (get (:allocations new-replica) j1))) 9)
                   (= (count (:c (get (:allocations new-replica) j1))) 9)
                   (= (count (:d (get (:allocations new-replica) j2))) 0)
                   (= (count (:e (get (:allocations new-replica) j2))) 0)
                   (= (count (:f (get (:allocations new-replica) j2))) 18))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 1, task A completes" true => true)

(def entry (create-log-entry :complete-task {:job j2 :task :f}))

(extensions/write-log-entry (:log env) entry)

(def replica-5
  (loop [replica replica-4]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 0)
                   (= (count (:b (get (:allocations new-replica) j1))) 18)
                   (= (count (:c (get (:allocations new-replica) j1))) 18)
                   (= (count (:d (get (:allocations new-replica) j2))) 0)
                   (= (count (:e (get (:allocations new-replica) j2))) 0)
                   (= (count (:f (get (:allocations new-replica) j2))) 0))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 2, task F completes" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task :b}))

(extensions/write-log-entry (:log env) entry)

(def replica-6
  (loop [replica replica-5]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 0)
                   (= (count (:b (get (:allocations new-replica) j1))) 0)
                   (= (count (:c (get (:allocations new-replica) j1))) 36)
                   (= (count (:d (get (:allocations new-replica) j2))) 0)
                   (= (count (:e (get (:allocations new-replica) j2))) 0)
                   (= (count (:f (get (:allocations new-replica) j2))) 0))
        (recur new-replica)
        new-replica))))

(fact "The peers rebalanced after job 1, task B completes" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task :c}))

(extensions/write-log-entry (:log env) entry)

(def replica-7
  (loop [replica replica-6]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 0)
                   (= (count (:b (get (:allocations new-replica) j1))) 0)
                   (= (count (:c (get (:allocations new-replica) j1))) 0)
                   (= (count (:d (get (:allocations new-replica) j2))) 0)
                   (= (count (:e (get (:allocations new-replica) j2))) 0)
                   (= (count (:f (get (:allocations new-replica) j2))) 0))
        (recur new-replica)
        new-replica))))

(fact "The peers stop working after job 1, task C completes" true => true)

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(component/stop env)

