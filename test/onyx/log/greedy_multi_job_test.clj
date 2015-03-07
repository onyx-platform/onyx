(ns onyx.log.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config
  {:zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id onyx-id})

(def peer-config
  {:zookeeper/address (:address (:zookeeper config))
   :onyx/id onyx-id
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :http-kit-websockets})

(def env (onyx.api/start-env env-config))

(def batch-size 20)

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-2
  [{:onyx/name :c
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :d
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def a-chan (chan))

(def b-chan (chan))

(def c-chan (chan))

(def d-chan (chan))

(defmethod l-ext/inject-lifecycle-resources :a
  [_ _] {:core.async/in-chan a-chan})

(defmethod l-ext/inject-lifecycle-resources :b
  [_ _] {:core.async/out-chan b-chan})

(defmethod l-ext/inject-lifecycle-resources :c
  [_ _] {:core.async/in-chan c-chan})

(defmethod l-ext/inject-lifecycle-resources :d
  [_ _] {:core.async/out-chan d-chan})

(def j1
  (onyx.api/submit-job
   peer-config
   {:workflow [[:a :b]]
    :catalog catalog-1
    :task-scheduler :onyx.task-scheduler/round-robin}))

(def j2
  (onyx.api/submit-job
   peer-config
   {:workflow [[:c :d]]
    :catalog catalog-2
    :task-scheduler :onyx.task-scheduler/round-robin}))

(prn "j1: " j1)

(prn "j2: " j2)

(def n-peers 4)

(def v-peers (onyx.api/start-peers n-peers peer-config))

(def ch (chan n-peers))

(def replica
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
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

(close! a-chan)
(close! b-chan)
(close! c-chan)
(close! d-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

