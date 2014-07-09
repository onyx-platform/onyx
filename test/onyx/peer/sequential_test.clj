(ns onyx.peer.sequential-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

(def hornetq-host "localhost")

(def hornetq-port 5446)

(def hornetq-cluster-name "onyx-cluster")

(def hornetq-group-address "231.7.7.7")

(def hornetq-refresh-timeout 5000)

(def hornetq-discovery-timeout 5000)

(def hornetq-group-port 9876)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow {:in {:inc :out}})

(defn run-job [in-queue out-queue n-messages batch-size echo]
  (hq-util/create-queue! hq-config in-queue)
  (hq-util/create-queue! hq-config out-queue)

  (let [id (str (java.util.UUID/randomUUID))
        coord-opts {:hornetq/mode :udp
                    :hornetq.udp/cluster-name hornetq-cluster-name
                    :hornetq.udp/group-address hornetq-group-address
                    :hornetq.udp/group-port hornetq-group-port
                    :hornetq.udp/refresh-timeout hornetq-refresh-timeout
                    :hornetq.udp/discovery-timeout hornetq-discovery-timeout
                    :zookeeper/address "127.0.0.1:2181"
                    :onyx/id id
                    :onyx.coordinator/revoke-delay 5000}
        peer-opts {:hornetq/mode :udp
                   :hornetq.udp/cluster-name hornetq-cluster-name
                   :hornetq.udp/group-address hornetq-group-address
                   :hornetq.udp/group-port hornetq-group-port
                   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
                   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
                   :zookeeper/address "127.0.0.1:2181"
                   :onyx/id id}
        catalog
        [{:onyx/name :in
          :onyx/ident :hornetq/read-segments
          :onyx/type :input
          :onyx/medium :hornetq
          :onyx/consumption :sequential
          :hornetq/queue-name in-queue
          :hornetq/host hornetq-host
          :hornetq/port hornetq-port
          :onyx/batch-size batch-size}

         {:onyx/name :inc
          :onyx/fn :onyx.peer.sequential-test/my-inc
          :onyx/type :transformer
          :onyx/consumption :sequential
          :onyx/batch-size batch-size}

         {:onyx/name :out
          :onyx/ident :hornetq/write-segments
          :onyx/type :output
          :onyx/medium :hornetq
          :onyx/consumption :sequential
          :hornetq/queue-name out-queue
          :hornetq/host hornetq-host
          :hornetq/port hornetq-port
          :onyx/batch-size batch-size}]
        conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts)
        v-peers (onyx.api/start-peers conn 1 peer-opts)]
    (hq-util/write-and-cap! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) echo)
    (onyx.api/submit-job conn {:catalog catalog :workflow workflow})
    (let [results (hq-util/read! hq-config out-queue (inc n-messages) echo)]
      (doseq [v-peer v-peers]
        (try
          ((:shutdown-fn v-peer))
          (catch Exception e (prn e))))
      (try
        (onyx.api/shutdown conn)
        (catch Exception e (prn e)))

      (fact results => (conj (vec (map (fn [x] {:n (inc x)}) (range n-messages))) :done)))))

(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 10 1 1)
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 100 10 10)
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 1000 100 100)
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 15000 1320 1000)

