(ns onyx.hornetq.embedded-test
  (:require [onyx.peer.pipeline-extensions :as p-ext]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def scheduler :onyx.job-scheduler/round-robin)

(def hq-servers
  ["hornetq/clustered-1.xml"
   "hornetq/clustered-2.xml"
   "hornetq/clustered-3.xml"])

(def hornetq-cluster-name "onyx-cluster")

(def hornetq-group-address "231.7.7.7")

(def hornetq-group-port 9876)

(def hornetq-refresh-timeout 5000)

(def hornetq-discovery-timeout 5000)

(def env-config
  {:hornetq/mode :udp
   :hornetq/server? true
   :hornetq.udp/cluster-name hornetq-cluster-name
   :hornetq.udp/group-address hornetq-group-address
   :hornetq.udp/group-port hornetq-group-port
   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
   :hornetq.server/type :embedded
   :hornetq.embedded/config hq-servers
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id
   :onyx.peer/job-scheduler scheduler})

(def peer-config
  {:hornetq/mode :udp
   :hornetq.udp/cluster-name hornetq-cluster-name
   :hornetq.udp/group-address hornetq-group-address
   :hornetq.udp/group-port hornetq-group-port
   :hornetq.udp/refresh-timeout hornetq-refresh-timeout
   :hornetq.udp/discovery-timeout hornetq-discovery-timeout
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id
   :onyx.peer/inbox-capacity (:inbox-capacity (:peer config))
   :onyx.peer/outbox-capacity (:outbox-capacity (:peer config))
   :onyx.peer/job-scheduler scheduler})

(def env (onyx.api/start-env env-config))

(def n-messages 2600)

(def batch-size 1320)

(def echo 100)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/ident :mem/read-segments
    :onyx/type :input
    :onyx/medium :memory
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :onyx/batch-size batch-size}

   {:onyx/name :inc
    :onyx/fn :onyx.hornetq.embedded-test/my-inc
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :mem/write-segments
    :onyx/type :output
    :onyx/medium :memory
    :onyx/consumption :concurrent
    :onyx/batch-size batch-size}])

(def workflow {:in {:inc :out}})

(def input (map (fn [x] {:n x}) (range n-messages)))

(def output (atom []))

(defmethod p-ext/apply-fn [:input :memory]
  [event] {:onyx.core/results input})

(defmethod p-ext/apply-fn [:output :memory]
  [{:keys [onyx.core/decompressed]}]
  {:onyx.core/results decompressed})

(defmethod p-ext/compress-batch [:output :memory]
  [{:keys [onyx.core/results]}]
  {:onyx.core/compressed results})

(defmethod p-ext/write-batch [:output :memory]
  [{:keys [onyx.core/compressed]}]
  (doseq [segment compressed]
    (swap! output conj segment))
  {})

(defmethod p-ext/seal-resource [:output :memory]
  [_]
  (swap! output conj :done)
  {})

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/round-robin})

(def p (promise))

(add-watch output :count
           (fn [_ _ _ state]
             (when (= (count state) (inc n-messages))
               (deliver p true))))

@p

(let [results @output
      expected (set (map (fn [x] {:n (inc x)}) (range n-messages)))]
  (fact (set (butlast results)) => expected)
  (fact (last results) => :done))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-env env)

