(ns onyx.hornetq.standalone-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

(def n-messages 15000)

(def batch-size 1320)

(def echo 1000)

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def (:host (:non-clustered (:hornetq config))) "localhost")

(def (:port (:non-clustered (:hornetq config))) 5465)

(def hq-config {"host" (:host (:non-clustered (:hornetq config))) "port" (:port (:non-clustered (:hornetq config)))})

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
    :onyx/consumption :sequential
    :hornetq/queue-name in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}
   
   {:onyx/name :inc
    :onyx/fn :onyx.hornetq.standalone-test/my-inc
    :onyx/type :transformer
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
    :onyx/batch-size batch-size}])

(def workflow {:in {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:hornetq/mode :standalone
                 :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
                 :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
                 :zookeeper/address "127.0.0.1:2181"
                 :onyx/id id
                 :onyx.coordinator/revoke-delay 5000})

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def peer-opts {:hornetq/mode :standalone
                :hornetq.standalone/host (:host (:non-clustered (:hornetq config)))
                :hornetq.standalone/port (:port (:non-clustered (:hornetq config)))
                :zookeeper/address "127.0.0.1:2181"
                :onyx/id id})

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-util/consume-queue! hq-config out-queue echo))

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

