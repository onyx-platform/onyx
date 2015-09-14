(ns onyx.log.peer-rebalance-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [playback-log get-counts load-config]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [clojure.test :refer [deftest is testing]]
            [schema.core :as s]
            [onyx.log.curator :as zk]))

(namespace-state-changes [(around :facts (s/with-fn-validation ?form))])

(facts

(def onyx-id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id onyx-id
    :onyx.peer/job-scheduler :onyx.job-scheduler/balanced))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def catalog-1
  [{:onyx/name :a
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-2
  [{:onyx/name :c
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :d
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Writes segments to a core.async channel"}])

(def a-chan (chan 100))

(def b-chan (chan (sliding-buffer 100)))

(def c-chan (chan 100))

(def d-chan (chan (sliding-buffer 100)))

(defn inject-a-ch [event lifecycle]
  {:core.async/chan a-chan})

(defn inject-b-ch [event lifecycle]
  {:core.async/chan b-chan})

(defn inject-c-ch [event lifecycle]
  {:core.async/chan c-chan})

(defn inject-d-ch [event lifecycle]
  {:core.async/chan d-chan})

(def a-calls
  {:lifecycle/before-task-start inject-a-ch})

(def b-calls
  {:lifecycle/before-task-start inject-b-ch})

(def c-calls
  {:lifecycle/before-task-start inject-c-ch})

(def d-calls
  {:lifecycle/before-task-start inject-d-ch})

(def lifecycles-1
  [{:lifecycle/task :a
    :lifecycle/calls :onyx.log.peer-rebalance-test/a-calls}
   {:lifecycle/task :a
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :b
    :lifecycle/calls :onyx.log.peer-rebalance-test/b-calls}
   {:lifecycle/task :b
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def lifecycles-2
  [{:lifecycle/task :c
    :lifecycle/calls :onyx.log.peer-rebalance-test/c-calls}
   {:lifecycle/task :c
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :d
    :lifecycle/calls :onyx.log.peer-rebalance-test/d-calls}
   {:lifecycle/task :d
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def j1
  (onyx.api/submit-job
   peer-config
   {:workflow [[:a :b]]
    :catalog catalog-1
    :lifecycles lifecycles-1
    :task-scheduler :onyx.task-scheduler/balanced}))

(def j2
  (onyx.api/submit-job
   peer-config
   {:workflow [[:c :d]]
    :catalog catalog-2
    :lifecycles lifecycles-2
    :task-scheduler :onyx.task-scheduler/balanced}))

(def n-peers 12)

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def ch (chan 10000))

(def replica-1
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 8000))

(is "the peers evenly balance" (get-counts replica-1 [j1 j2]) => [[3 3] [3 3]])

(def conn (zk/connect (:zookeeper/address (:env-config config))))

(def task-b (second (get-in replica-1 [:tasks (:job-id j1)])))

(def id (last (get (get (:allocations replica-1) (:job-id j1)) task-b)))

(zk/delete conn (str (onyx.log.zookeeper/pulse-path onyx-id) "/" id))

(zk/close conn)

(def replica-2 (playback-log (:log env) replica-1 ch 8000))

(is "the peers rebalance" (get-counts replica-2 [j1 j2]) => [[3 2] [3 3]])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(onyx.api/shutdown-peer-group peer-group)
)
