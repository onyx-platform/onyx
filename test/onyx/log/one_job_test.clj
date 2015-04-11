(ns onyx.log.one-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [onyx.extensions :as extensions]
            [onyx.test-helper :refer [playback-log]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api :as api]
            [midje.sweet :refer :all]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id onyx-id))

(def peer-config (assoc (:peer-config config) :onyx/id onyx-id))

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-peers 5)

(def batch-size 20)

(def v-peers (onyx.api/start-peers n-peers peer-group))

(def catalog
  [{:onyx/name :a
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :b
    :onyx/fn :onyx.log.one-job-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :c
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def in-chan (chan 100))

(def out-chan (chan (sliding-buffer 100)))

(defmethod l-ext/inject-lifecycle-resources :a
  [_ _] {:core.async/chan in-chan})

(defmethod l-ext/inject-lifecycle-resources :c
  [_ _] {:core.async/chan out-chan})

(defn my-inc [segment]
  {:n (inc (:n segment))})

(onyx.api/submit-job
 peer-config
 {:workflow [[:a :b] [:b :c]]
  :catalog catalog
  :task-scheduler :onyx.task-scheduler/balanced})

(def ch (chan n-peers))

(def replica
  (playback-log (:log env) (extensions/subscribe-to-log (:log env) ch) ch 2000))

(fact "peers balanced on 1 job"
      (into #{} (map count (mapcat vals (vals (:allocations replica))))) 
      =>
      #{1 2})

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(onyx.api/shutdown-peer-group peer-group)
