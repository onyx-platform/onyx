(ns onyx.queue.hornetq
  (:require [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defrecord HornetQ [host port]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting HornetQ")

    (let [config {"host" host "port" port}
          tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
          locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
          _ (.setConsumerWindowSize locator 0)
          session-factory (.createSessionFactory locator)]
      (assoc component
        :locator locator
        :session-factory session-factory)))

  (stop [{:keys [locator session-factory] :as component}]
    (taoensso.timbre/info "Stopping HornetQ")

    (.close session-factory)
    (.close locator)
    
    component))

(defn hornetq [host port]
  (map->HornetQ {:host host :port port}))

(defmethod extensions/create-tx-session HornetQ
  [queue]
  (let [session-factory (:session-factory queue)
        session (.createTransactedSession session-factory)]
    (.start session)
    session))

(defmethod extensions/create-producer HornetQ
  [queue session queue-name]
  (.createProducer session queue-name))

(defmethod extensions/create-consumer HornetQ
  [queue session queue-name]
  (.createConsumer session queue-name))

(defmethod extensions/create-queue HornetQ
  [queue task]
  (let [session (extensions/create-tx-session queue)
        ingress-queue (:ingress-queues task)
        egress-queues (vals (:egress-queues task))]
    (doseq [queue-name (conj egress-queues ingress-queue)]
      (try
        (.createQueue session queue-name queue-name true)
        (catch HornetQQueueExistsException e)))
    (.close session)))

(defmethod extensions/bootstrap-queue HornetQ
  [queue task]
  (let [session (extensions/create-tx-session queue)
        producer (extensions/create-producer queue session (:ingress-queues task))]
    (extensions/produce-message queue producer session (.array (fressian/write {})))
    (extensions/produce-message queue producer session (.array (fressian/write :done)))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue session)))

(defmethod extensions/produce-message HornetQ
  ([queue producer session msg]
     (let [message (.createMessage session true)]
       (.writeBytes (.getBodyBuffer message) msg)
       (.send producer message)))
  ([queue producer session msg group]
     (let [message (.createMessage session true)]
       (.putStringProperty message "_HQ_GROUP_ID" group)
       (.writeBytes (.getBodyBuffer message) msg)
       (.send producer message))))

(defmethod extensions/consume-message HornetQ
  [queue consumer]
  (.receive consumer))

(defmethod extensions/read-message HornetQ
  [queue message]
  (fressian/read (.toByteBuffer (.getBodyBuffer message))))

(defmethod extensions/ack-message HornetQ
  [queue message]
  (.acknowledge message))

(defmethod extensions/commit-tx HornetQ
  [queue session]
  (.commit session))

(defmethod extensions/close-resource HornetQ
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

