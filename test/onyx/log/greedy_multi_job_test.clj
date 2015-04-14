(ns onyx.log.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
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
    :task-scheduler :onyx.task-scheduler/balanced}))

(def j2
  (onyx.api/submit-job
   peer-config
   {:workflow [[:c :d]]
    :catalog catalog-2
    :task-scheduler :onyx.task-scheduler/balanced}))

(def n-peers 10)

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def ch (chan n-peers))

(def replica-1
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

(fact "20 peers were allocated to job 1, task A, 20 peers were allocated to job 1, task B" 
      (get-counts replica-1 [j1 j2]) => [[5 5] [0 0]])

(>!! a-chan :done)
(close! a-chan)

(def replica-2
  (playback-log (:log env) replica-1 ch 2000))

(fact "20 peers were reallocated to job 2, task C, 20 peers were reallocated to job 2, task D" 
      (get-counts replica-2 [j1 j2]) => [[] [5 5]])

(>!! c-chan :done)
(close! c-chan)

(def replica-3
  (playback-log (:log env) replica-2 ch 2000))

(fact "No peers are executing any tasks" (get-counts replica-3 [j1 j2]) => [[] []])

(close! b-chan)
(close! d-chan)

(onyx.api/shutdown-peer-group peer-group)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

