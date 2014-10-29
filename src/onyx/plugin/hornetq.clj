(ns onyx.plugin.hornetq
  (:require [clojure.data.fressian :as fressian]
            [onyx.coordinator.planning :as planning]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.operation :as operation]
            [onyx.queue.hornetq :refer [take-segments]]
            [onyx.extensions :as extensions]
            [dire.core :refer [with-post-hook!]]
            [taoensso.timbre :refer [debug]])
  (:import [org.hornetq.api.core SimpleString]
           [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defn read-batch [queue session-factory task]
  (let [session (.createTransactedSession session-factory)
        queue-name (:hornetq/queue-name task)
        consumer (.createConsumer session queue-name)]
    (.start session)
    (let [f (fn [] {:message (.receive consumer 1000)})
          rets (take-segments f (:onyx/batch-size task))
          rets (filter (fn [m] (not (nil? (:message m)))) rets)]
      (doseq [segment rets]
        (extensions/ack-message queue (:message segment)))
      {:onyx.core/batch (or rets [])
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
     :onyx.core/written? true}))

(defn read-batch-shim [{:keys [onyx.core/queue onyx.core/task-map] :as event}]
  (merge event (read-batch queue (:hornetq/session-factory event) task-map)))

(defn decompress-batch-shim [{:keys [onyx.core/batch] :as event}]
  (let [decompressed (map (comp decompress-segment :message) batch)]
    (merge event {:onyx.core/decompressed decompressed})))

(defn strip-sentinel-shim [event]
  (operation/on-last-batch
   event
   (fn [{:keys [onyx.core/task-map hornetq/session]}]
     (let [queue-name (:hornetq/queue-name task-map)
           query (.queueQuery session (SimpleString. queue-name))]
       (.getMessageCount query)))))

(defn requeue-sentinel-shim [{:keys [onyx.core/task-map] :as event}]
  (let [queue-name (:hornetq/queue-name task-map)]
    (let [session (:hornetq/session event)]
      (let [producer (.createProducer session queue-name)
            message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) (.array (fressian/write :done)))
        (.send producer message)
        (.close producer)))
    (merge event {:onyx.core/requeued? true})))

(defn apply-fn-in-shim [event]
  (merge event {:onyx.core/results (:onyx.core/decompressed event)}))

(defn apply-fn-out-shim [event]
  (merge event {:onyx.core/results (:onyx.core/decompressed event)}))

(defn compress-batch-shim [{:keys [onyx.core/results] :as event}]
  (merge event {:onyx.core/compressed (map compress-segment results)}))

(defn write-batch-shim
  [{:keys [onyx.core/task-map onyx.core/compressed] :as event}]
  (merge event (write-batch (:hornetq/session-factory event) task-map compressed)))

(defn seal-resource-shim [{:keys [onyx.core/task-map] :as event}]
  (let [queue-name (:hornetq/queue-name task-map)]
    (let [session (.createTransactedSession (:hornetq/session-factory event))]
      (let [producer (.createProducer session queue-name)
            message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) (.array (fressian/write :done)))
        (.send producer message)
        (.close producer))
      (.commit session)
      (.close session))))

(defmethod l-ext/start-lifecycle? :hornetq/read-segments
  [_ {:keys [hornetq/session onyx.core/ingress-queues onyx.core/task-map]}]
  {:onyx.core/start-lifecycle?
   (if (= (:task/consumption task-map) :sequential)
     (let [query (.queueQuery session (SimpleString. (:hornetq/queue-name task-map)))]
       (zero? (.getConsumerCount query)))
     true)})

(defmethod l-ext/inject-lifecycle-resources :hornetq/read-segments
  [_ {:keys [onyx.core/task-map] :as pipeline-data}]
  (let [config {"host" (:hornetq/host task-map) "port" (:hornetq/port task-map)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))]

    (when (= (:onyx/consumption task-map) :sequential)
      (.setConsumerWindowSize locator 0))

    (let [session-factory (.createSessionFactory locator)]
      (merge pipeline-data
             {:hornetq/locator locator
              :hornetq/session-factory session-factory}))))

(defmethod l-ext/close-temporal-resources :hornetq/read-segments
  [_ pipeline-data]
  (.commit (:hornetq/session pipeline-data))
  (.close (:hornetq/consumer pipeline-data))
  (.close (:hornetq/session pipeline-data))
  pipeline-data)

(defmethod l-ext/close-lifecycle-resources :hornetq/read-segments
  [_ pipeline-data]
  (.close (:hornetq/session-factory pipeline-data))
  (.close (:hornetq/locator pipeline-data))
  pipeline-data)

(defmethod p-ext/read-batch [:input :hornetq]
  [event] (read-batch-shim event))

(defmethod p-ext/decompress-batch [:input :hornetq]
  [event] (decompress-batch-shim event))

(defmethod p-ext/strip-sentinel [:input :hornetq]
  [event] (strip-sentinel-shim event))

(defmethod p-ext/requeue-sentinel [:input :hornetq]
  [event] (requeue-sentinel-shim event))

(defmethod p-ext/apply-fn [:input :hornetq]
  [event] (apply-fn-in-shim event))

(defmethod p-ext/apply-fn [:output :hornetq]
  [event] (apply-fn-out-shim event))

(defmethod p-ext/compress-batch [:output :hornetq]
  [event] (compress-batch-shim event))

(defmethod p-ext/write-batch [:output :hornetq]
  [event] (write-batch-shim event))

(defmethod l-ext/inject-lifecycle-resources :hornetq/write-segments
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [config {"host" (:hornetq/host task-map) "port" (:hornetq/port task-map)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))]

    (when (= (:onyx/consumption task-map) :sequential)
      (.setConsumerWindowSize locator 0))

    (let [session-factory (.createSessionFactory locator)]
      {:hornetq/locator locator
       :hornetq/session-factory session-factory})))

(defmethod l-ext/close-temporal-resources :hornetq/write-segments
  [_ pipeline-data]
  (.close (:hornetq/producer pipeline-data))
  (.close (:hornetq/session pipeline-data))
  {})

(defmethod l-ext/close-lifecycle-resources :hornetq/write-segments
  [_ pipeline-data]
  (.close (:hornetq/session-factory pipeline-data))
  (.close (:hornetq/locator pipeline-data))
  {})

(defmethod p-ext/seal-resource [:output :hornetq]
  [pipeline-data]
  (seal-resource-shim pipeline-data)
  {})

(with-post-hook! #'read-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/batch]}]
    (debug (format "[%s] Read %s segments" id (count batch)))))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/decompressed]}]
    (debug (format "[%s] Decompressed %s segments" id (count decompressed)))))

(with-post-hook! #'requeue-sentinel-shim
  (fn [{:keys [onyx.core/id]}]
    (debug (format "[%s] Requeued sentinel value" id))))

(with-post-hook! #'apply-fn-in-shim
  (fn [{:keys [onyx.core/id onyx.core/results]}]
    (debug (format "[%s] Applied fn to %s segments" id (count results)))))

(with-post-hook! #'apply-fn-out-shim
  (fn [{:keys [onyx.core/id onyx.core/results]}]
    (debug (format "[%s] Applied fn to %s segments" id (count results)))))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/compressed]}]
    (debug (format "[%s] Compressed batch of %s segments" id (count compressed)))))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/written?]}]
    (debug (format "[%s] Wrote batch with value" id written?))))

