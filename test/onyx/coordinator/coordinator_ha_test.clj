(ns onyx.coordinator.coordinator-ha-test
  (:require [midje.sweet :refer :all]
            [zookeeper ]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api])
  (:import [org.apache.curator.test TestingServer]
           [org.hornetq.core.server.embedded EmbeddedHornetQ]))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

;; Need to run ZK and HQ away from the Coordinator because the
;; Coordinator will start and stop these services as it
;; comes on and offline.
;;(def zk-server (TestingServer. (:spawn-port (:zookeeper config))))

(def hq-server
  (doto (EmbeddedHornetQ.)
    (.setConfigResourcePath
     (str (clojure.java.io/resource
           (first (:configs (:hornetq config))))))
    (.start)))

(def n-messages 2000)

(def batch-size 1320)

(def echo 1000)

(def id (str (java.util.UUID/randomUUID)))

(prn "ID is: " id)

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def onyx-port (+ 10000 (rand-int 10000)))

(def hq-config {"host" "localhost"
                "port" 5445})

(def coord-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address "localhost:2181"
   :onyx/id id
   :onyx.coordinator/host "localhost"
   :onyx.coordinator/port onyx-port
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address "localhost:2181"
   :onyx/id id})

(def onyx-server (onyx.api/start-distributed-coordinator coord-opts))

(def onyx-port-2 (+ 10000 (rand-int 10000)))

(def coord-opts-2
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address "localhost:2181"
   :onyx/id id
   :onyx.coordinator/host "localhost"
   :onyx.coordinator/port onyx-port-2
   :onyx.coordinator/revoke-delay 5000})

(def onyx-server-2 (future (onyx.api/start-distributed-coordinator coord-opts-2)))

(def conn (onyx.api/connect :distributed coord-opts))

(hq-util/create-queue! hq-config in-queue)
(hq-util/create-queue! hq-config out-queue)

(hq-util/write-and-cap! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) echo)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host "localhost"
    :hornetq/port 5445
    :onyx/batch-size batch-size}
   
   {:onyx/name :inc
    :onyx/fn :onyx.coordinator.coordinator-ha-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}
   
   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host "localhost"
    :hornetq/port 5445
    :onyx/batch-size batch-size}])

(def workflow {:in {:inc :out}})

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(Thread/sleep 10000)
(onyx.api/stop-distributed-coordinator onyx-server)

(def results (hq-util/consume-queue! hq-config out-queue echo))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)
(onyx.api/stop-distributed-coordinator onyx-server)

(.stop hq-server)
;;(.stop zk-server)

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))


