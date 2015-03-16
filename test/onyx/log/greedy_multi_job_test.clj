(ns onyx.log.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config (assoc (:peer-config config) :onyx/id onyx-id))

(def env (onyx.api/start-env env-config))

(def batch-size 20)

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-2
  [{:onyx/name :c
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :d
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
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

(def n-peers 40)

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
      (if-not (and (= (count (get-in new-replica [:allocations j1 task-a])) 20)
                   (= (count (get-in new-replica [:allocations j1 task-b])) 20)
                   (zero? (apply + (map count (vals (get (:allocations new-replica) j2))))))
        (recur new-replica)
        new-replica))))

(fact "20 peers were allocated to job 1, task A" true => true)

(fact "20 peers were allocated to job 1, task B" true => true)

(def task-a (first (get-in replica-1 [:tasks j1])))

(def task-b (second (get-in replica-1 [:tasks j1])))

(def task-c (first (get-in replica-1 [:tasks j2])))

(def task-d (second (get-in replica-1 [:tasks j2])))

(>!! a-chan :done)
(close! a-chan)

(def replica-2
  (loop [replica replica-1]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (and (= (count (get (get (:allocations new-replica) j2) task-c)) 20)
               (= (count (get (get (:allocations new-replica) j2) task-d)) 20))
        new-replica
        (recur new-replica)))))

(fact "20 peers were reallocated to job 2, task C" true => true)

(fact "20 peers were reallocated to job 2, task D" true => true)

(>!! c-chan :done)
(close! c-chan)

(def replica-3
  (loop [replica replica-2]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if (zero? (apply + (map count (vals (get (:allocations new-replica) j2)))))
        new-replica
        (recur new-replica)))))

(fact "No peers are executing any tasks" true => true)

(close! b-chan)
(close! d-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

