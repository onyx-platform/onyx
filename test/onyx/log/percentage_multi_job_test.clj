(ns onyx.log.percentage-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id id
    :onyx.peer/job-scheduler :onyx.job-scheduler/percentage))

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
    :percentage 70
    :task-scheduler :onyx.task-scheduler/round-robin}))

(def j2
  (onyx.api/submit-job
   peer-config
   {:workflow [[:c :d]]
    :catalog catalog-2
    :percentage 30
    :task-scheduler :onyx.task-scheduler/round-robin}))

(def n-peers 10)

(def v-peers-1 (onyx.api/start-peers n-peers peer-config))

(def ch (chan n-peers))

(def replica
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (apply concat (vals (get (:allocations new-replica) j1)))) 7)
                   (= (count (apply concat (vals (get (:allocations new-replica) j2)))) 3))
        (recur new-replica)
        new-replica))))

(fact "70/30% split for percentage job scheduler succeeded" true => true)

(def v-peers-2 (onyx.api/start-peers n-peers peer-config))

(def ch (chan n-peers))

(def replica
  (loop [replica (extensions/subscribe-to-log (:log env) ch)]
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

