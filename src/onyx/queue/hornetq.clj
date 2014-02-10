(ns onyx.queue.hornetq
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.planning :as planning]
            [onyx.peer.storage-api :as storage-api]
            [onyx.extensions :as extensions])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defrecord HornetQ [addr]
  component/Lifecycle

  (start [component]
    (prn "Starting HornetQ")

    (let [tc (TransportConfiguration. (.getName NettyConnectorFactory))
          locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
          session-factory (.createSessionFactory locator)]
      (assoc component :session-factory session-factory)))

  (stop [component]
    (prn "Stopping HornetQ")
    component))

(defn hornetq [addr]
  (map->HornetQ {:addr addr}))

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [element parent children phase]
  {:name (:onyx/name element)
   :ingress-queues (:hornetq/queue-name element)
   :egress-queues (planning/egress-queues-to-children children)
   :phase phase
   :consumption (:onyx/consumption element)})

(defmethod extensions/create-io-task
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [element parent children phase]
  {:name (:onyx/name element)
   :ingress-queues (get (:egress-queues parent) (:onyx/name element))
   :egress-queues {:self (:hornetq/queue-name element)}
   :phase phase
   :consumption (:onyx/consumption element)})

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

(defmethod extensions/produce-message HornetQ
  [queue producer session msg]
  (let [message (.createMessage session true)]
    (.writeString (.getBodyBuffer message) msg)
    (.send producer message)))

(defmethod extensions/consume-message HornetQ
  [queue consumer timeout]
  (.receive consumer timeout))

(defmethod extensions/read-message HornetQ
  [queue message]
  (.readString (.getBodyBuffer message)))

(defmethod extensions/ack-message HornetQ
  [queue message]
  (.acknowledge message))

(defmethod extensions/commit-tx HornetQ
  [queue session]
  (.commit session))

(defmethod extensions/close-resource HornetQ
  [queue resource]
  (.close resource))

(defmethod extensions/cap-queue HornetQ
  [queue task]
  (let [egress-queues (:egress-queues task)
        session (extensions/create-tx-session queue)]
    (doseq [queue-name egress-queues]
      (let [producer (extensions/create-producer session)
            message (.createMessage session true)]
        (.writeString (.getBufferBody message) (pr-str :done))
        (.send producer message)
        (.close producer)
        (.commit session)))
    (.close session)))

(defn read-batch [catalog task]
  (let [hq (hornetq (str (:hornetq/host task) ":" (:hornetq/port task)))
        session (extensions/create-tx-session hq)
        queue (:hornetq/queue-name task)
        consumer (.createConsumer session queue)]))

(defmethod storage-api/munge-read-batch
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [{:keys [catalog task-name] :as event}]
  (let [task (planning/find-task catalog task-name)
        batch (read-batch catalog task)]
    (assoc event :batch batch)))

(defmethod storage-api/munge-decompress-tx
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event])

(defmethod storage-api/munge-apply-fn
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event] event)

(defmethod storage-api/munge-apply-fn
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event] event)

(defmethod storage-api/munge-compress-tx
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event])

(defmethod storage-api/munge-write-batch
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event])

