(ns onyx.peer.killed-job-test
  (:require [clojure.core.async :refer [chan <!!]]
            [com.stuartsierra.component :as component]
            [midje.sweet :refer :all]
            [onyx.system :refer [onyx-development-env]]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.extensions :as extensions]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config (assoc (:env-config config) :onyx/id id))

(def peer-config (assoc (:peer-config config) :onyx/id id))

(def env (onyx.api/start-env env-config))

(def n-messages 15000)

(def batch-size 1320)

(def echo 1000)

(def in-queue-1 (str (java.util.UUID/randomUUID)))

(def out-queue-1 (str (java.util.UUID/randomUUID)))

(def in-queue-2 (str (java.util.UUID/randomUUID)))

(def out-queue-2 (str (java.util.UUID/randomUUID)))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(hq-util/create-queue! hq-config in-queue-1)
(hq-util/create-queue! hq-config out-queue-1)

(hq-util/create-queue! hq-config in-queue-2)
(hq-util/create-queue! hq-config out-queue-2)

;;; Don't write any segments to j1 so that the job will stay alive until we kill it.
(hq-util/write-and-cap! hq-config in-queue-2 (map (fn [x] {:n x}) (range n-messages)) echo)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog-1
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :hornetq/queue-name in-queue-1
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.killed-job-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue-1
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def catalog-2
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :hornetq/queue-name in-queue-2
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.killed-job-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue-2
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def workflow [[:in :inc] [:inc :out]])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(def j1 (onyx.api/submit-job
         peer-config
         {:catalog catalog-1 :workflow workflow
          :task-scheduler :onyx.task-scheduler/round-robin}))

(def j2 (onyx.api/submit-job
         peer-config
         {:catalog catalog-2 :workflow workflow
          :task-scheduler :onyx.task-scheduler/round-robin}))

(onyx.api/kill-job peer-config j1)

(def results (hq-util/consume-queue! hq-config out-queue-2 echo))

(def ch (chan 100))

;; Make sure we find the killed job in the replica, then bail
(loop [replica (extensions/subscribe-to-log (:log env) ch)]
  (let [position (<!! ch)
        entry (extensions/read-log-entry (:log env) position)
        new-replica (extensions/apply-log-entry entry replica)]
    (when-not (= (first (:killed-jobs new-replica)) j1)
      (recur new-replica))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(onyx.api/shutdown-env env)

