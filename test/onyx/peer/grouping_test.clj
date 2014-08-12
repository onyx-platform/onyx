(ns onyx.peer.grouping-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.api]
            [taoensso.timbre :refer [info]]))

(def output (atom nil))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

(def id (str (java.util.UUID/randomUUID)))

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def coord-opts
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :hornetq.server/type :embedded
   :hornetq.embedded/config (:configs (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :zookeeper/server? true
   :zookeeper.server/port (:spawn-port (:zookeeper config))
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name (:cluster-name (:hornetq config))
   :hornetq.udp/group-address (:group-address (:hornetq config))
   :hornetq.udp/group-port (:group-port (:hornetq config))
   :hornetq.udp/refresh-timeout (:refresh-timeout (:hornetq config))
   :hornetq.udp/discovery-timeout (:discovery-timeout (:hornetq config))
   :zookeeper/address (:address (:zookeeper config))
   :onyx/id id})

(def conn (onyx.api/connect :memory coord-opts))

(hq-util/create-queue! hq-config in-queue)
(hq-util/create-queue! hq-config out-queue)

(defn group-by-name [{:keys [name] :as segment}]
  name)

(defmethod l-ext/inject-lifecycle-resources
  :onyx.peer.grouping-test/sum-balance
  [_ event]
  (let [balance (atom {})]
    {:onyx.core/params [balance]
     :test/balance balance}))

(defmethod l-ext/close-lifecycle-resources
  :onyx.peer.grouping-test/sum-balance
  [_ {:keys [test/balance]}]
  (reset! output @balance)
  {})

(defn sum-balance [state {:keys [name amount] :as segment}]
  (swap! state (fn [v] (assoc v name (+ (get v name 0) amount))))
  [])

(def workflow {:in {:group-by-name {:sum-balance :out}}})

(def catalog
  [{:onyx/name :in
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq    
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 1000}

   {:onyx/name :group-by-name
    :onyx/fn :onyx.peer.grouping-test/group-by-name
    :onyx/type :grouper
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}

   {:onyx/name :sum-balance
    :onyx/ident :onyx.peer.grouping-test/sum-balance
    :onyx/fn :onyx.peer.grouping-test/sum-balance
    :onyx/type :aggregator
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}
   
   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size 1000}])

(def data
  (concat
   (map (fn [_] {:name "Mike" :amount 10}) (range 150))
   [{:name "Mike" :amount 10}
    {:name "Mike" :amount 15}
    {:name "Mike" :amount 20}

    {:name "Dorrene" :amount 30}
    {:name "Dorrene" :amount 40}

    {:name "Benti" :amount 55}]))

(hq-util/write-and-cap! hq-config in-queue data 100)

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

(fact @output => {"Mike" 1545 "Benti" 55 "Dorrene" 70})
(fact results => [:done])

