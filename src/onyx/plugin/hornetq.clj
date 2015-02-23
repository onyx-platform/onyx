(ns onyx.plugin.hornetq
  (:require [clojure.core.async :refer [go timeout <!]]
            [onyx.planning :as planning]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.operation :as operation]
            [onyx.messaging.acking-daemon :as acker]
            [onyx.extensions :as extensions]
            [dire.core :refer [with-post-hook!]]
            [taoensso.timbre :refer [debug]]
            [taoensso.nippy :as nippy])
  (:import [org.hornetq.api.core SimpleString]
           [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(def sentinel-byte-array (.limit (nippy/freeze :done) 32))

(defn take-segments
  ;; Set limit of 32 to match HornetQ's byte buffer. If they don't
  ;; match, hashCode() doesn't work as expected.
  ([f n] (take-segments f n []))
  ([f n rets]
     (if (= n (count rets))
       rets
       (let [segment (f)]
         (if (nil? (:message segment))
           rets
           (let [m (.toByteBuffer (.getBodyBufferCopy (:message segment)))]
             (if (= m sentinel-byte-array)
               (conj rets segment)
               (recur f n (conj rets segment)))))))))

(defmethod l-ext/start-lifecycle? :hornetq/read-segments
  [_ event]
  {:onyx.core/start-lifecycle? true})

(defmethod l-ext/inject-lifecycle-resources :hornetq/read-segments
  [_ {:keys [onyx.core/task-map] :as event}]
  (let [config {"host" (:hornetq/host task-map) "port" (:hornetq/port task-map)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))]
    (.setBlockOnAcknowledge locator true)
    (let [session-factory (.createSessionFactory locator)
          session (.createSession session-factory true true 0)
          consumer (.createConsumer session (:hornetq/queue-name task-map))]
      (.start session)
      {:hornetq/pending-messages (atom {})
       :hornetq/locator locator
       :hornetq/session-factory session-factory
       :hornetq/session session
       :hornetq/consumer consumer})))

(defmethod l-ext/close-temporal-resources :hornetq/read-segments
  [_ event] event)

(defmethod l-ext/close-lifecycle-resources :hornetq/read-segments
  [_ event]
  (.close (:hornetq/consumer event))
  (.close (:hornetq/session event))
  (.close (:hornetq/session-factory event))
  (.close (:hornetq/locator event))
  event)

(defmethod p-ext/read-batch [:input :hornetq]
  [{:keys [onyx.core/task-map hornetq/consumer hornetq/pending-messages] :as event}]
  (let [queue-name (:hornetq/queue-name task-map)
        timeout-ms (or (:onyx/batch-timeout task-map) 1000)]
    (let [f (fn [] {:id (acker/gen-message-id)
                   :message (.receive consumer timeout-ms)})
          rets (filter (comp not nil? :message)
                       (take-segments f (:onyx/batch-size task-map)))]
      (doseq [m rets]
        (.acknowledge (:message m))
        (swap! pending-messages assoc (:id m) (:message m)))
      {:onyx.core/batch (or rets [])})))

(defmethod p-ext/decompress-batch [:input :hornetq]
  [{:keys [onyx.core/batch]}]
  (let [decompress-f #(nippy/thaw (.toByteBuffer (.getBodyBufferCopy %)))]
    {:onyx.core/decompressed (map #(assoc % :message (decompress-f (:message %))) batch)}))

(defmethod p-ext/apply-fn [:input :hornetq]
  [event segment]
  segment)

(defmethod p-ext/ack-message [:input :hornetq]
  [{:keys [hornetq/pending-messages] :as event} message-id]
  (let [message (get @pending-messages message-id)]
;;    (prn "Acking " message)
    (.acknowledge message)
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/replay-message [:input :hornetq]
  [{:keys [hornetq/pending-messages] :as event} message-id]
  (.rollback (:hornetq/session event))
  (swap! pending-messages dissoc message-id))

(defmethod p-ext/pending? [:input :hornetq]
  [{:keys [hornetq/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? [:input :hornetq]
  [event message-id]
  (throw (Exception. "Not yet implemented")))

(defmethod p-ext/apply-fn [:output :hornetq]
  [event segment]
  segment)

(defmethod p-ext/compress-batch [:output :hornetq]
  [{:keys [onyx.core/results]}]
  (let [compress-f #(nippy/freeze (:message %))]
    {:onyx.core/compressed (map compress-f results)}))

(defmethod p-ext/write-batch [:output :hornetq]
  [{:keys [onyx.core/task-map onyx.core/compressed hornetq/session-factory]}]
  (let [session (.createTransactedSession session-factory)
        queue (:hornetq/queue-name task-map)
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

(defmethod l-ext/inject-lifecycle-resources :hornetq/write-segments
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [config {"host" (:hornetq/host task-map) "port" (:hornetq/port task-map)}
        tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)]
    {:hornetq/locator locator
     :hornetq/session-factory session-factory}))

(defmethod l-ext/close-temporal-resources :hornetq/write-segments
  [_ event]
  (.close (:hornetq/producer event))
  (.close (:hornetq/session event))
  {})

(defmethod l-ext/close-lifecycle-resources :hornetq/write-segments
  [_ event]
  (.close (:hornetq/session-factory event))
  (.close (:hornetq/locator event))
  {})

(defmethod p-ext/seal-resource [:output :hornetq]
  [{:keys [onyx.core/task-map] :as event}]
  (let [queue-name (:hornetq/queue-name task-map)]
    (let [session (.createTransactedSession (:hornetq/session-factory event))]
      (let [producer (.createProducer session queue-name)
            message (.createMessage session true)]
        (.writeBytes (.getBodyBuffer message) (nippy/freeze :done))
        (.send producer message)
        (.close producer))
      (.commit session)
      (.close session)))
  {})

