(ns onyx.peer.sequential-test
  (:require [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
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
   :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin})

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def dev (onyx-development-env env-config))

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow {:in {:inc :out}})

(defn run-job [in-queue out-queue n-messages batch-size echo]
  (let [env (component/start dev)
        id (str (java.util.UUID/randomUUID))
        catalog
        [{:onyx/name :in
          :onyx/ident :hornetq/read-segments
          :onyx/type :input
          :onyx/medium :hornetq
          :onyx/consumption :sequential
          :hornetq/queue-name in-queue
          :hornetq/host (:host (:non-clustered (:hornetq config)))
          :hornetq/port (:port (:non-clustered (:hornetq config)))
          :onyx/batch-size batch-size}

         {:onyx/name :inc
          :onyx/fn :onyx.peer.sequential-test/my-inc
          :onyx/type :function
          :onyx/consumption :sequential
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/ident :hornetq/write-segments
          :onyx/type :output
          :onyx/medium :hornetq
          :onyx/consumption :sequential
          :hornetq/queue-name out-queue
          :hornetq/host (:host (:non-clustered (:hornetq config)))
          :hornetq/port (:port (:non-clustered (:hornetq config)))
          :onyx/batch-size batch-size}]
        v-peers (onyx.api/start-peers! 1 peer-config)]

    (hq-util/create-queue! hq-config in-queue)
    (hq-util/create-queue! hq-config out-queue)

    (hq-util/write-and-cap! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) echo)
    (onyx.api/submit-job peer-config {:catalog catalog :workflow workflow
                                      :task-scheduler :onyx.task-scheduler/round-robin})

    (let [results (hq-util/consume-queue! hq-config out-queue echo)]
      (doseq [v-peer v-peers]
        (try
          ((:shutdown-fn v-peer))
          (catch Exception e (prn e))))
      (try
        (component/stop env)
        (catch Exception e (prn e)))

      (fact results => (conj (vec (map (fn [x] {:n (inc x)}) (range n-messages))) :done)))))

(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 10 1 1)
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 100 10 10)
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 1000 100 100)
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 15000 1320 1000)

