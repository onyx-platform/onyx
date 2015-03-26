(ns onyx.log.peer-rebalance-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.log.helper :refer [playback-log]]
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

(def peer-group (onyx.api/start-peer-group peer-config))

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

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def ch (chan 10000))

(defn get-counts [replica]
  (let [task-a (first (get-in replica [:tasks j1]))
        task-b (second (get-in replica [:tasks j1]))
        task-c (first (get-in replica [:tasks j2]))
        task-d (second (get-in replica [:tasks j2]))
        task-a-count (count (get (get (:allocations replica) j1) task-a))
        task-b-count (count (get (get (:allocations replica) j1) task-b))
        task-c-count (count (get (get (:allocations replica) j2) task-c)) 
        task-d-count (count (get (get (:allocations replica) j2) task-d))]
    [task-a-count task-b-count task-c-count task-d-count])) 

(def replica-1
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

(fact "the peers evenly balance" (get-counts replica-1) => [3 3 3 3])

(def conn (zk/connect (:zookeeper/address (:env-config config))))

(def task-b (second (get-in replica-1 [:tasks j1])))

(def id (last (get (get (:allocations replica-1) j1) task-b)))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" id))

(zk/close conn)

(def replica-2
  (playback-log (:log env) replica-1 ch 2000))

(fact "the peers rebalance" (get-counts replica-2) => [3 3 3 2])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(onyx.api/shutdown-peer-group peer-group)
