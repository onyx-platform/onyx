(ns ^:no-doc onyx.queue.hornetq
    (:require [clojure.string :refer [split]]
              [clojure.data.fressian :as fressian]
              [com.stuartsierra.component :as component]
              [onyx.extensions :as extensions]
              [taoensso.timbre :refer [info]])
    (:import [org.hornetq.api.core SimpleString]
             [org.hornetq.api.core TransportConfiguration]
             [org.hornetq.api.core HornetQQueueExistsException]
             [org.hornetq.api.core HornetQNonExistentQueueException]
             [org.hornetq.api.core DiscoveryGroupConfiguration]
             [org.hornetq.api.core UDPBroadcastGroupConfiguration]
             [org.hornetq.api.core.client HornetQClient]
             [org.hornetq.api.core.client ClientRequestor]
             [org.hornetq.api.core.management ManagementHelper]
             [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defmulti connect-to-locator :hornetq/mode)

(defn connect-standalone [host port]
  (let [config {"host" host "port" port}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)]
    (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))))

(defmethod connect-to-locator :standalone
  [{:keys [hornetq.standalone/host hornetq.standalone/port]}]
  (connect-standalone host port))

(defmethod connect-to-locator :multicast
  [{:keys [hornetq.multicast/cluster-name hornetq.multicast/group-address
           hornetq.multicast/group-port hornetq.multicast/refresh-timeout
           hornetq.multicast/discovery-timeout]}]
  (let [udp (UDPBroadcastGroupConfiguration. group-address group-port nil -1)
        gdc (DiscoveryGroupConfiguration. cluster-name refresh-timeout discovery-timeout udp)]
    (HornetQClient/createServerLocatorWithHA gdc)))

(defrecord HornetQClusteredConnection [opts]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HornetQ clustered connection")

    (let [locator (connect-to-locator opts)
          session-factory (.createSessionFactory locator)]
      (assoc component
        :locator locator
        :session-factory session-factory)))

  (stop [{:keys [locator session-factory] :as component}]
    (taoensso.timbre/info "Stopping HornetQ clustered connection")

    (.close session-factory)
    (.close locator)
    
    component))

(defn hornetq [opts]
  (map->HornetQClusteredConnection {:opts opts}))

(defmethod extensions/create-tx-session HornetQClusteredConnection
  [queue]
  (let [session-factory (:session-factory queue)
        session (.createTransactedSession session-factory)]
    (.start session)
    session))

(defmethod extensions/create-producer HornetQClusteredConnection
  [queue session queue-name]
  (extensions/create-queue-on-session queue session queue-name)
  (.createProducer session queue-name))

(defmethod extensions/create-consumer HornetQClusteredConnection
  [queue session queue-name]
  (extensions/create-queue-on-session queue session queue-name)
  (.createConsumer session queue-name))

(defmethod extensions/create-queue HornetQClusteredConnection
  [queue task]
  (let [session (extensions/create-tx-session queue)
        ingress-queue (:ingress-queues task)
        egress-queues (vals (:egress-queues task))]
    (doseq [queue-name (conj egress-queues ingress-queue)]
      (extensions/create-queue-on-session queue session queue-name))
    (.close session)))

(defmethod extensions/create-queue-on-session HornetQClusteredConnection
  [queue session queue-name]
  (try
    (.createQueue session queue-name queue-name true)
    (catch HornetQQueueExistsException e)
    (catch Exception e
      (info e))))

(defmethod extensions/n-messages-remaining HornetQClusteredConnection
  [queue session queue-name]
  (let [query (.queueQuery session (SimpleString. queue-name))]
    (.getMessageCount query)))

(defmethod extensions/n-consumers HornetQClusteredConnection
  [queue queue-name]
  (let [session (.createSession (:session-factory queue))
        requestor (ClientRequestor. session "jms.queue.hornetq.management")
        message (.createMessage session false)
        attr (format "core.clusterconnection.%s" (:cluster-name queue))]
    (ManagementHelper/putAttribute message attr "nodes")
    (.start session)

    (let [reply (.request requestor message)
          result (ManagementHelper/getResult reply)
          host-port-pairs
          (map (fn [x] (let [[h p] (split x #":")]
                        [(second (split h #"/")) p]))
               (vals result))
          locators (map (partial apply connect-standalone) host-port-pairs)
          session-factories (map #(.createSessionFactory %) locators)
          sessions (map (fn [sf] (let [s (.createSession sf)] (.start s) s)) session-factories)
          consumer-counts
          (doall
           (map (fn [s]
                  (.start s)
                  (let [query (.queueQuery s (SimpleString. queue-name))
                        n (.getConsumerCount query)]
                    (.close s)
                    n))
                sessions))]
      (.close session)
      (doall (map #(.close %) sessions))
      (doall (map #(.close %) session-factories))
      (doall (map #(.close %) locators))
      (apply + consumer-counts))))

(defmethod extensions/bootstrap-queue HornetQClusteredConnection
  [queue task]
  (let [session (extensions/create-tx-session queue)
        producer (extensions/create-producer queue session (:ingress-queues task))]
    (extensions/produce-message queue producer session (.array (fressian/write {})))
    (extensions/produce-message queue producer session (.array (fressian/write :done)))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue session)))

(defmethod extensions/produce-message HornetQClusteredConnection
  ([queue producer session msg]
     (let [message (.createMessage session true)]
       (.writeBytes (.getBodyBuffer message) msg)
       (.send producer message)))
  ([queue producer session msg group]
     (let [message (.createMessage session true)]
       (.putStringProperty message "_HQ_GROUP_ID" group)
       (.writeBytes (.getBodyBuffer message) msg)
       (.send producer message))))

(defmethod extensions/consume-message HornetQClusteredConnection
  [queue consumer]
  (.receive consumer))

(defmethod extensions/read-message HornetQClusteredConnection
  [queue message]
  (fressian/read (.toByteBuffer (.getBodyBuffer message))))

(defmethod extensions/ack-message HornetQClusteredConnection
  [queue message]
  (.acknowledge message))

(defmethod extensions/commit-tx HornetQClusteredConnection
  [queue session]
  (.commit session))

(defmethod extensions/close-resource HornetQClusteredConnection
  [queue resource]
  (.close resource))

(defn take-segments
  ([f n] (take-segments f n []))
  ([f n rets]
     (if (= n (count rets))
       rets
       (let [segment (f)]
         (if (nil? segment)
           rets
           (let [decompressed (fressian/read (.toByteBuffer (.getBodyBufferCopy segment)))]
             (if (= :done decompressed)
               (conj rets segment)
               (recur f n (conj rets segment)))))))))

