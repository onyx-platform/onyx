(ns onyx.log.peer-rebalance-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id onyx-id
    :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def env (onyx.api/start-env env-config))

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-2
  [{:onyx/name :c
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :d
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Writes segments to a core.async channel"}])

(def a-chan (chan 100))

(def b-chan (chan (sliding-buffer 100)))

(def c-chan (chan 100))

(def d-chan (chan (sliding-buffer 100)))

(defmethod l-ext/inject-lifecycle-resources :a
  [_ _] {:core.async/chan a-chan})

(defmethod l-ext/inject-lifecycle-resources :b
  [_ _] {:core.async/chan b-chan})

(defmethod l-ext/inject-lifecycle-resources :c
  [_ _] {:core.async/chan c-chan})

(defmethod l-ext/inject-lifecycle-resources :d
  [_ _] {:core.async/chan d-chan})

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

(def n-peers 12)

(def v-peers (onyx.api/start-peers n-peers peer-config))

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

(def conn (zk/connect (:zookeeper/address (:env-config config))))

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

