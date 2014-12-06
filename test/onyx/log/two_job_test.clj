(ns onyx.log.two-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
            [onyx.log.util :as util]
            [onyx.api :as api]
            [midje.sweet :refer :all]
            [zookeeper :as zk]))

(def onyx-id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def dev (onyx-development-env onyx-id (:env config)))

(def env (component/start dev))

(def peer-opts
  {:inbox-capacity 1000
   :outbox-capacity 1000
   :job-scheduler :onyx.job-scheduler/round-robin
   :state {:task-lifecycle-fn util/stub-task-lifecycle}})

(def n-peers 10)

(def v-peers (onyx.api/start-peers! onyx-id n-peers (:peer config) peer-opts))

(def catalog-1
  [{:onyx/name :a
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}

   {:onyx/name :b
    :onyx/fn :onyx.peer.single-peer-test/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent}

   {:onyx/name :c
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}])

(def catalog-2
  [{:onyx/name :d
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}

   {:onyx/name :e
    :onyx/fn :onyx.peer.single-peer-test/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent}

   {:onyx/name :f
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent}])

(onyx.api/submit-job (:log env)
                     {:workflow [[:a :b] [:b :c]]
                      :catalog catalog-1
                      :task-scheduler :onyx.task-scheduler/greedy})

(onyx.api/submit-job (:log env)
                     {:workflow [[:d :e] [:e :f]]
                      :catalog catalog-2
                      :task-scheduler :onyx.task-scheduler/greedy})

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:job-scheduler peer-opts)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)
          counts (map count (mapcat vals (vals (:allocations new-replica))))]
      (when-not (= counts [5 5])
        (recur new-replica)))))

(fact "peers balanced on 2 jobs" true => true)

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(component/stop env)

