(ns onyx.peer.bootstrap-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.api]))

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hornetq-cluster-name "onyx-cluster")

(def hornetq-group-address "231.7.7.7")

(def hornetq-refresh-timeout 5000)

(def hornetq-discovery-timeout 5000)

(def hornetq-group-port 9876)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-util/create-queue! hq-config out-queue)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow {:in-bootstrapped {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:hornetq/mode :multicast
                 :hornetq.multicast/cluster-name hornetq-cluster-name
                 :hornetq.multicast/group-address hornetq-group-address
                 :hornetq.multicast/group-port hornetq-group-port
                 :hornetq.multicast/refresh-timeout hornetq-refresh-timeout
                 :hornetq.multicast/discovery-timeout hornetq-discovery-timeout
                 :zookeeper/address "127.0.0.1:2181"
                 :onyx/id id
                 :onyx.coordinator/revoke-delay 5000})

(def peer-opts {:hornetq/mode :multicast
                :hornetq.multicast/cluster-name hornetq-cluster-name
                :hornetq.multicast/group-address hornetq-group-address
                :hornetq.multicast/group-port hornetq-group-port
                :hornetq.multicast/refresh-timeout hornetq-refresh-timeout
                :hornetq.multicast/discovery-timeout hornetq-discovery-timeout
                :zookeeper/address "127.0.0.1:2181"
                :onyx/id id})

(defmethod l-ext/apply-fn [:input :onyx-memory-test-plugin]
  [event] {:onyx.core/results [{:n 42}]})

(def catalog
  [{:onyx/name :in-bootstrapped
    :onyx/type :input
    :onyx/medium :onyx-memory-test-plugin
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :onyx/batch-size 2}

   {:onyx/name :inc
    :onyx/fn :onyx.peer.bootstrap-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 5}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 5}])

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-util/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact results => [{:n 43} :done])

