(ns onyx.log.round-robin-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [midje.sweet :refer :all]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.log.helper :refer [playback-log get-counts]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.extensions :as extensions]
            [onyx.api]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config
  (assoc (:peer-config config)
         :onyx/id onyx-id
         :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(try
  (def catalog-1
    [{:onyx/name :a
      :onyx/ident :core.async/read-from-chan
      :onyx/type :input
      :onyx/medium :core.async
      :onyx/batch-size 20
      :onyx/doc "Reads segments from a core.async channel"}

     {:onyx/name :b
      :onyx/fn :onyx.log.round-robin-multi-job-test/my-inc
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
      :onyx/fn :onyx.log.round-robin-multi-job-test/my-inc
      :onyx/type :function
      :onyx/batch-size 20}

     {:onyx/name :f
      :onyx/ident :core.async/write-to-chan
      :onyx/type :output
      :onyx/medium :core.async
      :onyx/batch-size 20
      :onyx/doc "Writes segments to a core.async channel"}])

  (def n-messages 1000)

  (def a-chan (chan (inc n-messages)))

  (def c-chan (chan (sliding-buffer (inc n-messages))))

  (def d-chan (chan (inc n-messages)))

  (def f-chan (chan (sliding-buffer (inc n-messages))))

  (defmethod l-ext/inject-lifecycle-resources :a
    [_ _] {:core.async/chan a-chan})

  (defmethod l-ext/inject-lifecycle-resources :c
    [_ _] {:core.async/chan c-chan})

  (defmethod l-ext/inject-lifecycle-resources :d
    [_ _] {:core.async/chan d-chan})

  (defmethod l-ext/inject-lifecycle-resources :f
    [_ _] {:core.async/chan f-chan})

  (defn my-inc [segment]
    (update-in segment [:n] inc))

  (def j1
    (onyx.api/submit-job
      peer-config
      {:workflow [[:a :b] [:b :c]]
       :catalog catalog-1
       :task-scheduler :onyx.task-scheduler/round-robin}))

  (def j2
    (onyx.api/submit-job
      peer-config
      {:workflow [[:d :e] [:e :f]]
       :catalog catalog-2
       :task-scheduler :onyx.task-scheduler/round-robin}))

  (def n-peers 36)

  (def v-peers (onyx.api/start-peers n-peers peer-group))

  (def ch (chan 10000))

  (def replica-1
    (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

  (fact "Peers balanced after before killed in multi-job test" 
        (get-counts replica-1 [j1 j2]) => [[6 6 6] [6 6 6]])

  (onyx.api/kill-job peer-config (:job-id j1))

  (def replica-2
    (playback-log (:log env) replica-1 ch 2000))

  (fact "Peers balanced after job killed in multi-job test" 
        (get-counts replica-2 [j1 j2]) => [[] [12 12 12]])

  (finally 
    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-env env)

    (onyx.api/shutdown-peer-group peer-group)))
