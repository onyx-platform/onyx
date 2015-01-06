(ns onyx.peer.automatic-kill-test
  (:require [clojure.core.async :refer [chan <!!]]
            [com.stuartsierra.component :as component]
            [midje.sweet :refer :all]
            [onyx.system :refer [onyx-development-env]]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.extensions :as extensions]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def env-config
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.server/type :embedded
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :hornetq.embedded/config (:configs (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-config
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy})

(def env (onyx.api/start-env env-config))

(def n-messages 15000)

(def batch-size 1320)

(def echo 1000)

(def in-queue-1 (str (java.util.UUID/randomUUID)))

(def out-queue-1 (str (java.util.UUID/randomUUID)))

(def in-queue-2 (str (java.util.UUID/randomUUID)))

(def out-queue-2 (str (java.util.UUID/randomUUID)))

(def in-queue-3 (str (java.util.UUID/randomUUID)))

(def out-queue-3 (str (java.util.UUID/randomUUID)))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(hq-util/create-queue! hq-config in-queue-1)
(hq-util/create-queue! hq-config out-queue-1)

(hq-util/create-queue! hq-config in-queue-2)
(hq-util/create-queue! hq-config out-queue-2)

(hq-util/create-queue! hq-config in-queue-3)
(hq-util/create-queue! hq-config out-queue-3)

(hq-util/write-and-cap! hq-config in-queue-1 (map (fn [x] {:n x}) (range n-messages)) echo)
(hq-util/write-and-cap! hq-config in-queue-2 (map (fn [x] {:n x}) (range n-messages)) echo)
(hq-util/write-and-cap! hq-config in-queue-3 (map (fn [x] {:n x}) (range n-messages)) echo)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog-1
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue-1
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.automatic-kill-test/my-invalid-fn-name
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue-1
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def catalog-2
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name "bad-queue-name"
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.automatic-kill-test/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue-2
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def catalog-3
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue-3
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.automatic-kill-test/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue-3
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

(def j3 (onyx.api/submit-job
         peer-config
         {:catalog catalog-3 :workflow workflow
          :task-scheduler :onyx.task-scheduler/round-robin}))

(def results (hq-util/consume-queue! hq-config out-queue-3 echo))

(def ch (chan 100))

(extensions/subscribe-to-log (:log env) 0 ch)

;; Make sure we find the first two killed jobs in the replica, then bail
(loop [replica {:job-scheduler (:onyx.peer/job-scheduler peer-config)}]
  (let [position (<!! ch)
        entry (extensions/read-log-entry (:log env) position)
        new-replica (extensions/apply-log-entry entry replica)]
    (when-not (= (:killed-jobs new-replica) [j1 j2])
      (recur new-replica))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown v-peer))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(onyx.api/shutdown env)

