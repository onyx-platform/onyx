(ns onyx.peer.distributed-test
  (:require [com.stuartsierra.component :as component]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.coordinator.distributed :as server]
            [onyx.api]))

(def n-messages 10)

(def batch-size 5)

(def echo 1)

(def hornetq-host "localhost")

(def hornetq-port 5465)

(def hornetq-cluster-name "onyx-cluster")

(def hornetq-group-address "231.7.7.7")

(def hornetq-refresh-timeout 5000)

(def hornetq-discovery-timeout 5000)

(def hornetq-group-port 9876)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def onyx-port (+ 10000 (rand-int 10000)))

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-util/create-queue! hq-config in-queue)
(hq-util/create-queue! hq-config out-queue)

(def before-msgs (mapv (fn [x] {:n x}) (range n-messages)))

(hq-util/write-and-cap! hq-config in-queue before-msgs echo)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent    
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size}
   
   {:onyx/name :inc
    :onyx/fn :onyx.peer.distributed-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}
   
   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size}])

(def workflow {:in {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:hornetq/mode :udp
                 :hornetq.udp/cluster-name hornetq-cluster-name
                 :hornetq.udp/group-address hornetq-group-address
                 :hornetq.udp/group-port hornetq-group-port
                 :hornetq.udp/refresh-timeout hornetq-refresh-timeout
                 :hornetq.udp/discovery-timeout hornetq-discovery-timeout
                 :zookeeper/address "127.0.0.1:2181"
                 :onyx/id id
                 :onyx.coordinator/port onyx-port
                 :onyx.coordinator/revoke-delay 5000})

(def onyx-server (component/start (server/coordinator-server coord-opts)))

(def conn (onyx.api/connect (str "onyx:distributed//localhost:" onyx-port "/" id) coord-opts))

(def peer-opts {:hornetq/mode :udp
                :hornetq.udp/cluster-name hornetq-cluster-name
                :hornetq.udp/group-address hornetq-group-address
                :hornetq.udp/group-port hornetq-group-port
                :hornetq.udp/refresh-timeout hornetq-refresh-timeout
                :hornetq.udp/discovery-timeout hornetq-discovery-timeout
                :zookeeper/address "127.0.0.1:2181"
                :onyx/id id})

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-util/read! hq-config out-queue (inc n-messages) echo))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)
(component/stop onyx-server)

(fact results => (conj (vec (map (fn [x] {:n (inc x)}) (range n-messages))) :done))

