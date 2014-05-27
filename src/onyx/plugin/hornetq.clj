(ns onyx.plugin.hornetq
  (:require [clojure.data.fressian :as fressian]
            [onyx.coordinator.planning :as planning]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.queue.hornetq :refer [take-segments]]
            [onyx.extensions :as extensions]
            [dire.core :refer [with-post-hook!]]
            [taoensso.timbre :refer [info]])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

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
    (.commit session)
    {:hornetq/session session
     :hornetq/producer producer
     :written? true}))

(defn read-batch-shim [{:keys [catalog task] :as event}]
  (let [task-map (planning/find-task catalog task)]
    (merge event (read-batch (:hornetq/session-factory event) catalog task-map))))

(defn decompress-batch-shim [{:keys [batch] :as event}]
  (merge event {:decompressed (map decompress-segment batch)}))

(defn requeue-sentinel-shim [{:keys [task catalog] :as event}]
  (let [task (planning/find-task catalog task)
        queue-name (:hornetq/queue-name task)]
    (let [session (.createTransactedSession (:hornetq/session-factory event))]
      (let [producer (.createProducer session queue-name)
            message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) (.array (fressian/write :done)))
        (.send producer message)
        (.close producer))
      (.commit session)
      (.close session))
    (merge event {:requeued? true})))

(defn ack-batch-shim [{:keys [queue batch] :as event}]
  (doseq [message batch]
    (extensions/ack-message queue message))
  (merge event {:acked (count batch)}))

(defn apply-fn-in-shim [event]
  (merge event {:results (:decompressed event)}))

(defn apply-fn-out-shim [event]
  (merge event {:results (:decompressed event)}))

(defn compress-batch-shim [{:keys [results] :as event}]
  (merge event {:compressed (map compress-segment results)}))

(defn write-batch-shim [{:keys [catalog task compressed] :as event}]
  (let [task-map (planning/find-task catalog task)]
    (merge event (write-batch (:hornetq/session-factory event) task-map compressed))))

(defn seal-resource-shim [{:keys [catalog task] :as event}]
  (let [task (planning/find-task catalog task)
        queue-name (:hornetq/queue-name task)]
    (let [session (.createTransactedSession (:hornetq/session-factory event))]
      (let [producer (.createProducer session queue-name)
            message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) (.array (fressian/write :done)))
        (.send producer message)
        (.close producer))
      (.commit session)
      (.close session))))

(defmethod p-ext/inject-pipeline-resources :hornetq/read-segments
  [pipeline-data]
  (let [task (planning/find-task (:catalog pipeline-data) (:task pipeline-data))
        config {"host" (:hornetq/host task) "port" (:hornetq/port task)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        _ (.setConsumerWindowSize locator 0)
        session-factory (.createSessionFactory locator)]
    (merge pipeline-data
           {:hornetq/locator locator
            :hornetq/session-factory session-factory})))

(defmethod p-ext/close-temporal-resources :hornetq/read-segments
  [pipeline-data]
  (.commit (:hornetq/session pipeline-data))
  (.close (:hornetq/consumer pipeline-data))
  (.close (:hornetq/session pipeline-data))
  pipeline-data)

(defmethod p-ext/close-pipeline-resources :hornetq/read-segments
  [pipeline-data]
  (.close (:hornetq/session-factory pipeline-data))
  (.close (:hornetq/locator pipeline-data))
  pipeline-data)

(defmethod p-ext/read-batch [:input :hornetq]
  [event] (read-batch-shim event))

(defmethod p-ext/decompress-batch [:input :hornetq]
  [event] (decompress-batch-shim event))

(defmethod p-ext/requeue-sentinel [:input :hornetq]
  [event] (requeue-sentinel-shim event))

(defmethod p-ext/ack-batch [:input :hornetq]
  [event] (ack-batch-shim event))

(defmethod p-ext/apply-fn [:input :hornetq]
  [event] (apply-fn-in-shim event))

(defmethod p-ext/apply-fn [:output :hornetq]
  [event] (apply-fn-out-shim event))

(defmethod p-ext/ack-batch [:output :hornetq]
  [event] (ack-batch-shim event))

(defmethod p-ext/compress-batch [:output :hornetq]
  [event] (compress-batch-shim event))

(defmethod p-ext/write-batch [:output :hornetq]
  [event] (write-batch-shim event))

(defmethod p-ext/inject-pipeline-resources :hornetq/write-segments
  [pipeline-data]
  (let [task (planning/find-task (:catalog pipeline-data) (:task pipeline-data))
        config {"host" (:hornetq/host task) "port" (:hornetq/port task)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        _ (.setConsumerWindowSize locator 0)
        session-factory (.createSessionFactory locator)]
    {:hornetq/locator locator
     :hornetq/session-factory session-factory}))

(defmethod p-ext/close-temporal-resources :hornetq/write-segments
  [pipeline-data]
  (.close (:hornetq/producer pipeline-data))
  (.close (:hornetq/session pipeline-data))
  {})

(defmethod p-ext/close-pipeline-resources :hornetq/write-segments
  [pipeline-data]
  (.close (:hornetq/session-factory pipeline-data))
  (.close (:hornetq/locator pipeline-data))
  {})

(defmethod p-ext/seal-resource [:output :hornetq]
  [pipeline-data]
  (seal-resource-shim pipeline-data)
  {})

(with-post-hook! #'read-batch-shim
  (fn [{:keys [id batch]}]
    (info (format "[%s] Read %s segments" id (count batch)))))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [id decompressed]}]
    (info (format "[%s] Decompressed %s segments" id (count decompressed)))))

(with-post-hook! #'requeue-sentinel-shim
  (fn [{:keys [id]}]
    (info (format "[%s] Requeued sentinel value" id))))

(with-post-hook! #'apply-fn-in-shim
  (fn [{:keys [id results]}]
    (info (format "[%s] Applied fn to %s segments" id (count results)))))

(with-post-hook! #'ack-batch-shim
  (fn [{:keys [id acked]}]
    (info (format "[%s] Acked %s segments" id acked))))

(with-post-hook! #'apply-fn-out-shim
  (fn [{:keys [id results]}]
    (info (format "[%s] Applied fn to %s segments" id (count results)))))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [id compressed]}]
    (info (format "[%s] Compressed batch of %s segments" id (count compressed)))))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [id written?]}]
    (info (format "[%s] Wrote batch with value" id written?))))

