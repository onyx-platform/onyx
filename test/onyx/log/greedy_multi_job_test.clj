(ns onyx.log.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [playback-log get-counts]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config]]
            [onyx.api :as api]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (load-config))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config (assoc (:peer-config config) :onyx/id onyx-id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def batch-size 20)

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/ident :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-2
  [{:onyx/name :c
    :onyx/ident :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :d
    :onyx/ident :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
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
    :lifecycle/calls :onyx.log.greedy-multi-job-test/a-calls}
   {:lifecycle/task :a
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :b
    :lifecycle/calls :onyx.log.greedy-multi-job-test/b-calls}
   {:lifecycle/task :b
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def lifecycles-2
  [{:lifecycle/task :c
    :lifecycle/calls :onyx.log.greedy-multi-job-test/c-calls}
   {:lifecycle/task :c
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :d
    :lifecycle/calls :onyx.log.greedy-multi-job-test/d-calls}
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

(def n-peers 10)

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def ch (chan n-peers))

(def replica-1
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

(fact "5 peers were allocated to job 1, task A, 5 peers were allocated to job 1, task B" 
      (get-counts replica-1 [j1 j2]) => (fn [x] (or (= (sort x) [[0 0]] [5 5])
                                                    (= (sort x) [[] [5 5]]))))

(>!! a-chan :done)
(close! a-chan)

(def replica-2
  (playback-log (:log env) replica-1 ch 2000))

(fact "5 peers were reallocated to job 2, task C, 5 peers were reallocated to job 2, task D" 
      (get-counts replica-2 [j1 j2]) => (fn [x] (or (= (sort x) [[0 0] [5 5]])
                                                    (= (sort x) [[] [5 5]]))))

(>!! c-chan :done)
(close! c-chan)

(def replica-3
  (playback-log (:log env) replica-2 ch 2000))

(fact "No peers are executing any tasks" (get-counts replica-3 [j1 j2])
      => (fn [x] (or (= x [[0 0] [0 0]])
                     (= x [[] []]))))

(close! b-chan)
(close! d-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

