(ns onyx.peer.bootstrap-test
  (:require [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config
  (assoc (:peer-config config)
    :onyx/id id
    :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin))

(def env (onyx.api/start-env env-config))

(def out-queue (str (java.util.UUID/randomUUID)))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow {:in-bootstrapped {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(hq-util/create-queue! hq-config out-queue)

(defmethod p-ext/apply-fn [:input :onyx-memory-test-plugin]
  [event] {:onyx.core/results [{:n 42}]})

(def catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/medium :onyx-memory-test-plugin
    :onyx/bootstrap? true
    :onyx/batch-size 2}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.bootstrap-test/my-inc
    :onyx/type :function
    :onyx/batch-size 5}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 5}])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-util/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

(fact results => [{:n 43} :done])

