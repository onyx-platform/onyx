(ns onyx.peer.join-test
  (:require [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.api]))

(def config (read-string (slurp (clojure.java.io/resource "test-config.edn"))))

(def batch-size 2)

(def echo 1)

(def id (str (java.util.UUID/randomUUID)))

(def name-queue (str (java.util.UUID/randomUUID)))

(def age-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def hq-config {"host" (:host (:non-clustered (:hornetq config)))
                "port" (:port (:non-clustered (:hornetq config)))})

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

(hq-util/create-queue! hq-config name-queue)

(hq-util/create-queue! hq-config age-queue)

(hq-util/create-queue! hq-config out-queue)

(def people
  [{:id 1 :name "Mike" :age 23}
   {:id 2 :name "Dorrene" :age 24}
   {:id 3 :name "Benti" :age 23}
   {:id 4 :name "John" :age 19}
   {:id 5 :name "Shannon" :age 31}
   {:id 6 :name "Kristen" :age 25}])

(def names (map #(select-keys % [:id :name]) people))

(def ages (map #(select-keys % [:id :age]) people))

(hq-util/write-and-cap! hq-config name-queue names 1)

(hq-util/write-and-cap! hq-config age-queue ages 1)

(defn join-person [local-state segment]
  (let [state @local-state]
    (if-let [record (get state (:id segment))]
      (let [result (merge record segment)]
        (swap! local-state dissoc (:id segment))
        result)
      (do (swap! local-state assoc (:id segment) (dissoc segment :id))
          []))))

(def catalog
  [{:onyx/name :names
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name name-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}
   
   {:onyx/name :ages
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name age-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}
   
   {:onyx/name :join-person
    :onyx/fn :onyx.peer.join-test/join-person
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/group-by-key :id
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host (:host (:non-clustered (:hornetq config)))
    :hornetq/port (:port (:non-clustered (:hornetq config)))
    :onyx/batch-size batch-size}])

(def workflow
  [[:names :join-person]
   [:ages :join-person]
   [:join-person :out]])

(defmethod l-ext/inject-lifecycle-resources :join-person
  [_ event]
  {:onyx.core/params [(atom {})]})

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-util/consume-queue! hq-config out-queue echo))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact (into #{} (butlast results)) => (into #{} people))

