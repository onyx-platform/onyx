(ns onyx.queue.hornetq
  (:require [com.stuartsierra.component :as component]
            [onyx.coordinator.planning :as planning]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
            [dire.core :refer [with-post-hook!]]
            [taoensso.timbre :refer [info]])
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
  [queue egress-queues]
  (let [session (extensions/create-tx-session queue)]
    (doseq [queue-name egress-queues]
      (let [producer (extensions/create-producer queue session queue-name)
            message (.createMessage session true)]
        (.writeString (.getBodyBuffer message) (pr-str :done))
        (.send producer message)
        (.close producer)))
    (.commit session)
    (.close session)))

;;;;;;;;;;;;;;;;;;;;; To be split out into a library ;;;;;;;;;;;;;;;;;;;;;

(defn read-batch [catalog task]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory))
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createSession session-factory)
        queue (:hornetq/queue-name task)
        consumer (.createConsumer session queue)]
    (.start session)
    (let [f #(when-let [m (.receive consumer (:hornetq/timeout task))]
               (.acknowledge m)
               m)
          rets (doall (repeatedly (:hornetq/batch-size task) f))]
      (.close consumer)
      (.close session)
      (filter identity rets))))

(defn decompress-segment [segment]
  (read-string (.readString (.getBodyBuffer segment))))

(defn compress-segment [segment]
  (pr-str segment))

(defn write-batch [task compressed]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory))
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createSession session-factory)
        queue (:hornetq/queue-name task)
        producer (.createProducer session queue)]
    (.start session)
    (doseq [x compressed]
      (let [message (.createMessage session true)]
        (.writeString (.getBodyBuffer message) x)
        (.send producer message)))
    (.commit session)
    (.close producer)
    (.close session)))

(defn read-batch-shim [{:keys [catalog task]}]
  (let [task-map (planning/find-task catalog task)
        batch (read-batch catalog task-map)]
    {:batch batch}))

(defn decompress-batch-shim [{:keys [batch]}]
  {:decompressed (map decompress-segment batch)})

(defn apply-fn-in-shim [event]
  {:results (:decompressed event)})

(defn apply-fn-out-shim [event]
  {:results (:decompressed event)})

(defn compress-batch-shim [{:keys [results]}]
  {:compressed (map compress-segment results)})

(defn write-batch-shim [{:keys [catalog task compressed]}]
  (let [task-map (planning/find-task catalog task)]
    (write-batch task-map compressed)
    {:written? true}))

(defmethod p-ext/read-batch
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event] (read-batch-shim event))

(defmethod p-ext/decompress-batch
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event] (decompress-batch-shim event))

(defmethod p-ext/apply-fn
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event] (apply-fn-in-shim event))

(defmethod p-ext/apply-fn
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event] (apply-fn-out-shim event))

(defmethod p-ext/compress-batch
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event] (compress-batch-shim event))

(defmethod p-ext/write-batch
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event] (write-batch-shim event))

(with-post-hook! #'read-batch-shim
  (fn [{:keys [batch]}]
    (info "HornetQ ingress: Read" (count batch) "segments")))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [decompressed]}]
    (info "HornetQ ingress: Decompressed" (count decompressed) "segments")))

(with-post-hook! #'apply-fn-in-shim
  (fn [{:keys [results]}]
    (info "HornetQ ingress: Applied fn to" (count results) "segments")))

(with-post-hook! #'apply-fn-out-shim
  (fn [{:keys [results]}]
    (info "HornetQ egress: Applied fn to" (count results) "segments")))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [compressed]}]
    (info "HornetQ egress: Compressed batch of" (count compressed) "segments")))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [written?]}]
    (info "HornetQ egress: Wrote batch with value" written?)))

;;;;;;;;;;;;;;;;;;; End library ;;;;;;;;;;;;;;;;;;;;;;;;

