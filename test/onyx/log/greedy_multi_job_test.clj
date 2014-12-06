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

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(def peer-opts
  {:inbox-capacity 1000
   :outbox-capacity 1000
   :job-scheduler :onyx.job-scheduler/greedy
   :state {:task-lifecycle-fn util/stub-task-lifecycle}})

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}

   {:onyx/name :b
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}])

(def catalog-2
  [{:onyx/name :c
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}

   {:onyx/name :d
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}])

(def j1
  (onyx.api/submit-job (:log env)
                       {:workflow [[:a :b]]
                        :catalog catalog-1
                        :task-scheduler :onyx.task-scheduler/greedy}))

(def j2
  (onyx.api/submit-job (:log env)
                       {:workflow [[:c :d]]
                        :catalog catalog-2
                        :task-scheduler :onyx.task-scheduler/greedy}))

(def n-peers 40)

(def v-peers (onyx.api/start-peers! onyx-id n-peers (:peer config) peer-opts))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:job-scheduler peer-opts)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations new-replica) j1))) 40)
                   (zero? (apply + (map count (vals (get (:allocations new-replica) j2))))))
        (recur new-replica)
        new-replica))))

(fact "40 peers were allocated to job 1, task A" true => true)

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
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(component/stop env)

