(ns onyx.peer.grouping-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.api]))

(def hornetq-host "localhost")

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(defn group-by-name [{:keys [name] :as segment}]
  name)

(defmethod p-ext/inject-pipeline-resources
  :onyx.peer.grouping-test/sum-balance
  [event]
  (let [balance (atom {})]
    {:params [balance]
     :balance balance}))

(defmethod p-ext/close-pipeline-resources
  :onyx.peer.grouping-test/sum-balance
  [{:keys [balance]}]
  (prn "The total balance was: " @balance))

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
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :hornetq/batch-size 1000}

   {:onyx/name :group-by-name
    :onyx/fn :onyx.peer.grouping-test/group-by-name
    :onyx/type :grouper
    :onyx/consumption :concurrent
    :onyx/batch-size 1000}

   {:onyx/name :sum-balance
    :onyx/ident :onyx.peer.grouping-test/sum-balance
    :onyx/fn :onyx.peer.grouping-test/sum-balance
    :onyx/type :transformer
    :onyx/consumption :sequential
    :onyx/batch-size 1000}
   
   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 1000}])

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

(def data
  [{:name "Mike" :amount 10}
   {:name "Mike" :amount 15}
   {:name "Mike" :amount 20}

   {:name "Dorrene" :amount 30}
   {:name "Dorrene" :amount 40}

   {:name "Benti" :amount 55}])

(hq-util/write-and-cap! hq-config in-queue data 1)

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 7 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

#_(def results (hq-util/read! hq-config out-queue (inc (count data)) 1))

#_(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

#_(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))


