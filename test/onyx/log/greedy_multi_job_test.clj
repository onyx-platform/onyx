(ns onyx.log.greedy-multi-job-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.log.entry :refer [create-log-entry]]
            [onyx.extensions :as extensions]
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
   :job-scheduler :onyx.job-scheduler/greedy})

(def j1
  (onyx.api/submit-job (:log env)
                       {:workflow [[:a :b]]
                        :catalog []
                        :task-scheduler :onyx.task-scheduler/greedy}))

(def j2
  (onyx.api/submit-job (:log env)
                       {:workflow [[:c :d]]
                        :catalog []
                        :task-scheduler :onyx.task-scheduler/greedy}))

(def n-peers 40)

(def v-peers (onyx.api/start-peers! onyx-id n-peers (:peer config) peer-opts))

(def ch (chan n-peers))

(extensions/subscribe-to-log (:log env) 0 ch)

(def replica
  (loop [replica {:job-scheduler (:job-scheduler peer-opts)}]
    (let [position (<!! ch)
          entry (extensions/read-log-entry (:log env) position)
          new-replica (extensions/apply-log-entry entry replica)]
      (if-not (and (= (count (:a (get (:allocations replica) j1))) 40)
                   (zero? (apply + (map count (vals (get (:allocations replica) j2))))))
        (recur new-replica)
        new-replica))))

(fact "40 peers were allocated to job 1, task A" true => true)

(def entry (create-log-entry :complete-task {:job j1 :task :a}))

(extensions/write-log-entry (:log env) entry)

(loop [replica replica]
  (let [position (<!! ch)
        entry (extensions/read-log-entry (:log env) position)
        new-replica (extensions/apply-log-entry entry replica)]
    (if (and (= (count (:b (get (:allocations new-replica) j1))) 40)
             (zero? (apply + (map count (vals (get (:allocations new-replica) j2))))))
      new-replica
      (recur new-replica))))

(fact "All peers were reallocated to job 1, task B" true => true)

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(component/stop env)

