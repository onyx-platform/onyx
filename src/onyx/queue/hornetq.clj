(ns onyx.queue.hornetq
  (:require [clojure.data.fressian :as fressian]
            [com.stuartsierra.component :as component]
            [onyx.coordinator.planning :as planning]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
            [dire.core :refer [with-post-hook!]]
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
    (.writeBytes (.getBodyBuffer message) msg)
    (.send producer message)))

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

(defmethod extensions/cap-queue HornetQ
  [queue egress-queues]
  (let [session (extensions/create-tx-session queue)]
    (doseq [queue-name egress-queues]
      (let [producer (extensions/create-producer queue session queue-name)
            message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) (.array (fressian/write :done)))
        (.send producer message)
        (.close producer)))
    (.commit session)
    (.close session)))

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

;;;;;;;;;;;;;;;;;;;;; To be split out into a library ;;;;;;;;;;;;;;;;;;;;;

(defn read-batch [session-factory catalog task]
  (let [session (.createTransactedSession session-factory)
        queue (:hornetq/queue-name task)
        consumer (.createConsumer session queue)]
    (.start session)
    (let [f #(.receive consumer)
          rets (doall (take-segments f (:hornetq/batch-size task)))]
      {:batch (or rets [])
       :hornetq/session session
       :hornetq/consumer consumer})))

(defn decompress-segment [segment]
  (fressian/read (.toByteBuffer (.getBodyBuffer segment))))

(defn compress-segment [segment]
  (.array (fressian/write segment)))

(defn write-batch [session-factory task compressed]
  (let [session (.createTransactedSession session-factory)
        queue (:hornetq/queue-name task)
        producer (.createProducer session queue)]
    (.start session)
    (doseq [x compressed]
      (let [message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) x)
        (.send producer message)))
    {:hornetq/session session
     :hornetq/producer producer
     :written? true}))

(defn read-batch-shim [{:keys [catalog task] :as event}]
  (let [task-map (planning/find-task catalog task)]
    (read-batch (:hornetq/session-factory event) catalog task-map)))

(defn decompress-batch-shim [{:keys [batch]}]
  {:decompressed (map decompress-segment batch)})

(defn ack-ingress-batch-shim [{:keys [queue batch] :as event}]
  (doseq [message batch]
    (extensions/ack-message queue message))

  (.commit (:hornetq/session event))
  (.close (:hornetq/consumer event))
  (.close (:hornetq/session event))
  {:acked (count batch)})

(defn ack-egress-batch-shim [{:keys [queue batch] :as event}]
  (doseq [message batch]
    (extensions/ack-message queue message))

  (.commit (:hornetq/session event))
  (.close (:hornetq/producer event))
  (.close (:hornetq/session event))
  {})

(defn apply-fn-in-shim [event]
  {:results (:decompressed event)})

(defn apply-fn-out-shim [event]
  {:results (:decompressed event)})

(defn compress-batch-shim [{:keys [results]}]
  {:compressed (map compress-segment results)})

(defn write-batch-shim [{:keys [catalog task compressed] :as event}]
  (let [task-map (planning/find-task catalog task)]
    (write-batch (:hornetq/session-factory event) task-map compressed)))

(defmethod p-ext/inject-pipeline-resources
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [pipeline-data]
  (let [task (planning/find-task (:catalog pipeline-data) (:task pipeline-data))
        config {"host" (:hornetq/host task) "port" (:hornetq/port task)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        _ (.setConsumerWindowSize locator 0)
        session-factory (.createSessionFactory locator)]
    {:hornetq/locator locator
     :hornetq/session-factory session-factory}))

(defmethod p-ext/close-pipeline-resources
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [pipeline-data]
  (.close (:hornetq/session-factory pipeline-data))
  (.close (:hornetq/locator pipeline-data))
  {})

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

(defmethod p-ext/ack-batch
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event] (ack-ingress-batch-shim event))

(defmethod p-ext/apply-fn
  {:onyx/type :queue
   :onyx/direction :input
   :onyx/medium :hornetq}
  [event] (apply-fn-in-shim event))

(defmethod p-ext/ack-batch
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [event] (ack-egress-batch-shim event))

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

(defmethod p-ext/inject-pipeline-resources
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [pipeline-data]
  (let [task (planning/find-task (:catalog pipeline-data) (:task pipeline-data))
        config {"host" (:hornetq/host task) "port" (:hornetq/port task)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        _ (.setConsumerWindowSize locator 0)
        session-factory (.createSessionFactory locator)]
    {:hornetq/locator locator
     :hornetq/session-factory session-factory}))

(defmethod p-ext/close-pipeline-resources
  {:onyx/type :queue
   :onyx/direction :output
   :onyx/medium :hornetq}
  [pipeline-data]
  (.commit (:hornetq/session pipeline-data))
  (.close (:hornetq/producer pipeline-data))
  (.close (:hornetq/session pipeline-data))
  (.close (:hornetq/session-factory pipeline-data))
  (.close (:hornetq/locator pipeline-data))
  {})

(with-post-hook! #'read-batch-shim
  (fn [{:keys [batch]}]
    (info "[HornetQ ingress] Read" (count batch) "segments")))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [decompressed]}]
    (info "[HornetQ ingress] Decompressed" (count decompressed) "segments")))

(with-post-hook! #'apply-fn-in-shim
  (fn [{:keys [results]}]
    (info "[HornetQ ingress] Applied fn to" (count results) "segments")))

(with-post-hook! #'ack-ingress-batch-shim
  (fn [{:keys [acked]}]
    (info "[HornetQ igress] Acked" acked "segments")))

(with-post-hook! #'ack-egress-batch-shim
  (fn [{:keys [acked]}]
    (info "[HornetQ egress] Acked" acked "segments")))

(with-post-hook! #'apply-fn-out-shim
  (fn [{:keys [results]}]
    (info "[HornetQ egress] Applied fn to" (count results) "segments")))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [compressed]}]
    (info "[HornetQ egress] Compressed batch of" (count compressed) "segments")))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [written?]}]
    (info "[HornetQ egress] Wrote batch with value" written?)))

;;;;;;;;;;;;;;;;;;; End library ;;;;;;;;;;;;;;;;;;;;;;;;

