(ns onyx.peer.multi-peer-test
  (:require [midje.sweet :refer :all]
            [onyx.api])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(def k 150)

(def in-queue (str (java.util.UUID/randomUUID)))

(def out-queue (str (java.util.UUID/randomUUID)))

(def tc (TransportConfiguration. (.getName NettyConnectorFactory)))

(def locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc])))

(def session-factory (.createSessionFactory locator))

(def session (.createTransactedSession session-factory))

(.start session)

(.createQueue session in-queue in-queue true)

(.createQueue session out-queue out-queue true)

(def producer (.createProducer session in-queue))

(doseq [n (range k)]
  (let [message (.createMessage session true)]
    (.writeString (.getBodyBuffer message) (pr-str {:n n}))
    (.send producer message)))

(def sentinel (.createMessage session true))
(.writeString (.getBodyBuffer sentinel) (pr-str :done))
(.send producer sentinel)

(.commit session)
(.close producer)
(.close session)

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def catalog
  [{:onyx/name :in
    :onyx/direction :input
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name in-queue
    :hornetq/host "localhost"
    :hornetq/port 5445
    :hornetq/batch-size 50
    :hornetq/timeout 5}
   {:onyx/name :inc
    :onyx/fn :onyx.peer.multi-peer-test/my-inc
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 50
    :onyx/timeout 5}
   {:onyx/name :out
    :onyx/direction :output
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host "localhost"
    :hornetq/port 5445
    :hornetq/batch-size 50
    :hornetq/timeout 5}])

(def workflow {:in {:inc :out}})

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:datomic-uri (str "datomic:mem://" id)
                 :hornetq-addr "localhost:5445"
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 2000})

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def peer-opts {:hornetq-addr "localhost:5445"
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

(def v-peers (onyx.api/start-peers conn 3 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def session (.createTransactedSession session-factory))

(.start session)

(def consumer (.createConsumer session out-queue "" 0 64000 false))

(def results (atom []))

(doseq [n (range (inc k))]
  (let [message (.receive consumer)]
    (.acknowledge message)
    (swap! results conj (read-string (.readString (.getBodyBuffer message))))))

(.commit session)
(.close consumer)
(.close session)

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

(prn @results)

(let [expected-ns (vec (map (fn [x] {:n x}) (range 1 (inc k))))]
  (fact (into #{} @results) => (into #{} (conj expected-ns :done))))

