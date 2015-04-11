(ns onyx.log.two-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [onyx.test-helper :refer [playback-log get-counts]]
            ; Add for generative testing later
            ;[onyx.log.generative-test :as log-gen-test]
            ;[clojure.test.check.generators :as gen]
            ;[clojure.test :refer :all]
            ;[com.gfredericks.test.chuck.clojure-test :refer [checking]]
            ;[onyx.messaging.aeron :as aeron]
            [com.stuartsierra.component :as component]
            [clojure.test :refer :all]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id onyx-id
    :onyx.peer/job-scheduler :onyx.job-scheduler/balanced))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers 12)

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/fn :onyx.log.two-job-test/my-inc
    :onyx/type :function
    :onyx/batch-size 20}

   {:onyx/name :c
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-2
  [{:onyx/name :d
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :e
    :onyx/fn :onyx.log.two-job-test/my-inc
    :onyx/type :function
    :onyx/batch-size 20}

   {:onyx/name :f
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 20
    :onyx/doc "Writes segments to a core.async channel"}])

(def my-inc identity)

(def a-chan (chan 100))

(def c-chan (chan (sliding-buffer 100)))

(def d-chan (chan 100))

(def f-chan (chan (sliding-buffer 100)))

(defmethod l-ext/inject-lifecycle-resources :a
  [_ _] {:core.async/chan a-chan})

(defmethod l-ext/inject-lifecycle-resources :c
  [_ _] {:core.async/chan c-chan})

(defmethod l-ext/inject-lifecycle-resources :d
  [_ _] {:core.async/chan d-chan})

(defmethod l-ext/inject-lifecycle-resources :f
  [_ _] {:core.async/chan f-chan})

(def j1 
  (onyx.api/submit-job
    peer-config
    {:workflow [[:a :b] [:b :c]]
     :catalog catalog-1
     :task-scheduler :onyx.task-scheduler/balanced}))

(def j2 
  (onyx.api/submit-job
    peer-config
    {:workflow [[:d :e] [:e :f]]
     :catalog catalog-2
     :task-scheduler :onyx.task-scheduler/balanced}))

(def ch (chan n-peers))

(def replica
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

(fact "peers balanced on 2 jobs" (get-counts replica [j1 j2]) => [[2 2 2] [2 2 2]])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
