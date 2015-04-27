(ns onyx.log.percentage-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [playback-log get-counts load-config]]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config
  (assoc (:peer-config config)
         :onyx/id onyx-id
         :onyx.peer/job-scheduler :onyx.job-scheduler/percentage))

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

(defn inject-a-ch [event lifecycle]
  {:core.async/chan a-chan})

(defn inject-b-ch [event lifecycle]
  {:core.async/chan b-chan})

(defn inject-c-ch [event lifecycle]
  {:core.async/chan c-chan})

(defn inject-d-ch [event lifecycle]
  {:core.async/chan d-chan})

(def a-calls
  {:lifecycle/before-task :onyx.peer.min-peers-test/inject-a-ch})

(def b-calls
  {:lifecycle/before-task :onyx.peer.min-peers-test/inject-b-ch})

(def c-calls
  {:lifecycle/before-task :onyx.peer.min-peers-test/inject-c-ch})

(def d-calls
  {:lifecycle/before-task :onyx.peer.min-peers-test/inject-d-ch})

(def lifecycles-1
  [{:lifecycle/task :a
    :lifecycle/calls :onyx.peer.min-peers-test/a-calls}
   {:lifecycle/task :a
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :b
    :lifecycle/calls :onyx.peer.min-peers-test/b-calls}
   {:lifecycle/task :b
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def lifecycles-2
  [{:lifecycle/task :c
    :lifecycle/calls :onyx.peer.min-peers-test/c-calls}
   {:lifecycle/task :c
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :d
    :lifecycle/calls :onyx.peer.min-peers-test/d-calls}
   {:lifecycle/task :d
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def j1
  (onyx.api/submit-job
   peer-config
   {:workflow [[:a :b]]
    :catalog catalog-1
    :lifecycles lifecycles-1
    :percentage 70
    :task-scheduler :onyx.task-scheduler/balanced}))

(def j2
  (onyx.api/submit-job
   peer-config
   {:workflow [[:c :d]]
    :catalog catalog-2
    :lifecycles lifecycles-2
    :percentage 30
    :task-scheduler :onyx.task-scheduler/balanced}))

(def n-peers 10)

(def v-peers-1 (onyx.api/start-peers n-peers peer-group))

(def ch (chan 10000))

(def replica
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

(fact "70/30% split for percentage job scheduler succeeded" 
      (map (partial apply +) 
           (get-counts replica [j1 j2])) => [7 3])

(def v-peers-2 (onyx.api/start-peers n-peers peer-group))

(def replica-2
  (playback-log (:log env) replica ch 2000))

(fact "70/30% split for percentage job scheduler succeeded after rebalance" 
      (map (partial apply +) 
           (get-counts replica-2 [j1 j2])) => [14 6])

(doseq [v-peer v-peers-1]
  (onyx.api/shutdown-peer v-peer))

(doseq [v-peer v-peers-2]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(onyx.api/shutdown-peer-group peer-group)
