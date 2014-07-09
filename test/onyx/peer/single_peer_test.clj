(ns onyx.peer.single-peer-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

(def n-messages 15000)

(def batch-size 1320)

(def echo 1000)

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def hornetq-host "localhost")

(def hornetq-port 5446)

(def hornetq-cluster-name "onyx-cluster")

(def hornetq-group-address "231.7.7.7")

(def hornetq-group-port 9876)

(def hornetq-refresh-timeout 5000)

(def hornetq-discovery-timeout 5000)

(def hq-config {"host" hornetq-host "port" hornetq-port})

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
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size batch-size}
   
   {:onyx/name :inc
    :onyx/fn :onyx.peer.single-peer-test/my-inc
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
                 :onyx.coordinator/revoke-delay 5000})

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

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

(try
  ;; (dorun (map deref (map :runner v-peers)))
  (finally
   (doseq [v-peer v-peers]
     (try
       ((:shutdown-fn v-peer))
       (catch Exception e (prn e))))
   (try
     (onyx.api/shutdown conn)
     (catch Exception e (prn e)))))

(let [expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

