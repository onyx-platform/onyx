(ns onyx.peer.single-peer-test
  (:require [midje.sweet :refer :all]
            [onyx.peer.hornetq-util :as hq-util]
            [onyx.api]))

(def timeout 500)

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow {:in {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:datomic-uri (str "datomic:mem://" id)
                 :hornetq-host hornetq-host
                 :hornetq-port hornetq-port
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 5000})

(def peer-opts {:hornetq-host hornetq-host
                :hornetq-port hornetq-port
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

(defn run-job [in-queue out-queue n-messages batch-size echo]
  (hq-util/write-and-cap! hq-config in-queue (map (fn [x] {:n x}) (range n-messages)) echo)

  (let [catalog
        [{:onyx/name :in
          :onyx/direction :input
          :onyx/consumption :concurrent
          :onyx/type :queue
          :onyx/medium :hornetq
          :hornetq/queue-name in-queue
          :hornetq/host hornetq-host
          :hornetq/port hornetq-port
          :hornetq/batch-size batch-size
          :hornetq/timeout timeout}
         
         {:onyx/name :inc
          :onyx/fn :onyx.peer.single-peer-test/my-inc
          :onyx/type :transformer
          :onyx/consumption :concurrent
          :onyx/batch-size batch-size
          :onyx/timeout timeout}
         
         {:onyx/name :out
          :onyx/direction :output
          :onyx/consumption :concurrent
          :onyx/type :queue
          :onyx/medium :hornetq
          :hornetq/queue-name out-queue
          :hornetq/host hornetq-host
          :hornetq/port hornetq-port
          :onyx/batch-size batch-size
          :onyx/timeout timeout}]
        conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts)
        v-peers (onyx.api/start-peers conn 1 peer-opts)]
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
(run-job (str (java.util.UUID/randomUUID)) (str (java.util.UUID/randomUUID)) 100000 3000 10000)

