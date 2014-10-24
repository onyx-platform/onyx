(ns ^:no-doc onyx.peer.transform
    (:require [clojure.core.async :refer [chan >! go alts!! close!]]
              [clojure.data.fressian :as fressian]
              [onyx.peer.task-lifecycle-extensions :as l-ext]
              [onyx.peer.pipeline-extensions :as p-ext]
              [onyx.queue.hornetq :refer [take-segments]]
              [onyx.coordinator.planning :refer [find-task]]
              [onyx.peer.operation :as operation]
              [onyx.extensions :as extensions]
              [taoensso.timbre :refer [debug]]
              [dire.core :refer [with-post-hook!]])
    (:import [java.util UUID]
             [java.security MessageDigest]))

(defn requeue-sentinel [queue session queue-name uuid]
  (let [p (extensions/create-producer queue session queue-name)]
    (extensions/produce-message queue p session (.array (fressian/write :done)) {:uuid uuid})
    (extensions/close-resource queue p)))

(defn seal-queue [queue queue-names]
  (let [session (extensions/create-tx-session queue)]
    (doseq [queue-name queue-names]
      (let [producer (extensions/create-producer queue session queue-name)]
        (extensions/produce-message queue producer session (.array (fressian/write :done)))
        (extensions/close-resource queue producer)))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue session)))

(defn reader-thread [event queue reader-ch input consumer]
  (go
   (try
    (loop []
      (let [segment (extensions/consume-message queue consumer)]
        (when segment
          (>! reader-ch {:input input :message segment})
          (recur))))
    (finally
     (close! reader-ch)))))

(defn read-batch [queue consumers task-map event]
  (if-not (empty? consumers)
    (let [reader-chs (map (fn [_] (chan (:onyx/batch-size task-map))) (vals consumers))
          reader-threads (doall (map (fn [ch [input consumer]]
                                       (reader-thread event queue ch input consumer))
                                     reader-chs consumers))
          read-f #(first (alts!! (conj reader-chs (clojure.core.async/timeout 200))))
          segments (doall (take-segments read-f (:onyx/batch-size task-map)))]

      ;; Ack each of the segments. Closing a consumer with unacked tasks will send
      ;; all the tasks back onto the queue.
      (doseq [segment segments]
        (extensions/ack-message queue (:message segment)))

      ;; Close each consumer. If any consumers are left open with unacked messages,
      ;; the next round of reading will skip the messages that the consumers are hanging
      ;; onto - hence breaking the sequential reads for task that require it.
      (doseq [consumer (vals consumers)]
        (extensions/close-resource queue consumer))
      segments)
    []))

(defn decompress-segment [queue message]
  (extensions/read-message queue message))

(defn hash-value [x]
  (let [md5 (MessageDigest/getInstance "MD5")]
    (apply str (.digest md5 (.getBytes (pr-str x) "UTF-8")))))

(defn group-message [segment catalog task]
  (let [t (find-task catalog task)]
    (if-let [k (:onyx/group-by-key t)]
      (hash-value (get segment k))
      (when-let [f (:onyx/group-by-fn t)]
        (hash-value ((operation/resolve-fn {:onyx/fn f}) segment))))))

(defn compress-segment [next-tasks catalog segment]
  {:compressed (.array (fressian/write segment))
   :hash-group (reduce (fn [groups t]
                         (assoc groups t (group-message segment catalog t)))
                       {} next-tasks)})

(defn write-batch [queue session producers msgs catalog queue-name->task]
  (dorun
   (for [p producers msg msgs]
     (let [queue-name (extensions/producer->queue-name queue p)
           task (get queue-name->task queue-name)]
       (if-let [group (get (:hash-group msg) task)]
         (extensions/produce-message queue p session (:compressed msg) {:group group})
         (extensions/produce-message queue p session (:compressed msg))))))
  {:onyx.core/written? true})

(defn read-batch-shim
  [{:keys [onyx.core/queue onyx.core/session onyx.core/ingress-queues
           onyx.core/catalog onyx.core/task-map] :as event}]
  (let [drained (:drained-inputs @(:onyx.core/pipeline-state event))
        consumer-f (partial extensions/create-consumer queue session)
        uncached (into {} (remove (fn [[t _]] (get drained t)) ingress-queues))
        consumers (reduce-kv (fn [all k v] (assoc all k (consumer-f v))) {} uncached)
        batch (read-batch queue consumers task-map event)]
    (merge event {:onyx.core/batch batch :onyx.core/consumers consumers})))

(defn decompress-batch-shim [{:keys [onyx.core/queue onyx.core/batch] :as event}]
  (let [decompressed-msgs (map (partial decompress-segment queue) (map :message batch))]
    (merge event {:onyx.core/decompressed decompressed-msgs})))

(defn strip-sentinel-shim [event]
  (operation/on-last-batch
   event
   (fn [{:keys [onyx.core/queue onyx.core/session onyx.core/ingress-queues]}]
     (let [task-name (:input (last (:onyx.core/batch event)))
           queue-name (get ingress-queues task-name)]
       (extensions/n-messages-remaining queue session queue-name)))))

(defn requeue-sentinel-shim
  [{:keys [onyx.core/queue onyx.core/session onyx.core/ingress-queues] :as event}]
  (let [uuid (or (extensions/message-uuid queue (:message (last (:onyx.core/batch event))))
                 (UUID/randomUUID))]
    (let [task-name (:input (last (:onyx.core/batch event)))
          queue-name (get ingress-queues task-name)]
      (requeue-sentinel queue session queue-name uuid)))
  (merge event {:onyx.core/requeued? true}))

(defn apply-fn-shim
  [{:keys [onyx.core/decompressed onyx.transform/fn onyx.core/params
           onyx.core/task-map] :as event}]
  (if (:onyx/transduce? task-map)
    (merge event {:onyx.core/results (sequence (apply fn params) decompressed)})
    (let [results (flatten (map (partial operation/apply-fn fn params) decompressed))]
      (merge event {:onyx.core/results results}))))

(defn compress-batch-shim
  [{:keys [onyx.core/results onyx.core/catalog onyx.core/serialized-task] :as event}]
  (let [next-tasks (keys (:task/children-links serialized-task))
        compressed-msgs (map (partial compress-segment next-tasks catalog) results)]
    (merge event {:onyx.core/compressed compressed-msgs})))

(defn write-batch-shim
  [{:keys [onyx.core/queue onyx.core/egress-queues onyx.core/serialized-task
           onyx.core/catalog onyx.core/session onyx.core/compressed] :as event}]
  (let [queue-name->task (clojure.set/map-invert (:task/children-links serialized-task))
        producers (map (partial extensions/create-producer queue session) egress-queues)
        batch (write-batch queue session producers compressed catalog queue-name->task)]
    (merge event {:onyx.core/producers producers})))

(defn seal-resource-shim [{:keys [onyx.core/queue onyx.core/egress-queues] :as event}]
  (merge event (seal-queue queue egress-queues)))

(defmethod l-ext/start-lifecycle? :transformer
  [_ event]
  {:onyx.core/start-lifecycle? (operation/start-lifecycle? event)})

(defmethod l-ext/inject-lifecycle-resources :transformer
  [_ {:keys [onyx.core/task-map]}]
  {:onyx.transform/fn (operation/resolve-fn task-map)})

(defmethod p-ext/read-batch :default
  [event] (read-batch-shim event))

(defmethod p-ext/decompress-batch :default
  [event] (decompress-batch-shim event))

(defmethod p-ext/strip-sentinel :default
  [event] (strip-sentinel-shim event))

(defmethod p-ext/requeue-sentinel :default
  [event] (requeue-sentinel-shim event))

(defmethod p-ext/apply-fn :default
  [event] (apply-fn-shim event))

(defmethod p-ext/compress-batch :default
  [event] (compress-batch-shim event))

(defmethod p-ext/write-batch :default
  [event] (write-batch-shim event))

(defmethod p-ext/seal-resource :default
  [event] (seal-resource-shim event))

(with-post-hook! #'read-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/batch onyx.core/consumers]}]
    (debug (format "[%s] Read batch of %s segments" id (count batch)))))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/decompressed]}]
    (debug (format "[%s] Decompressed %s segments" id (count decompressed)))))

(with-post-hook! #'requeue-sentinel-shim
  (fn [{:keys [onyx.core/id]}]
    (debug (format "[%s] Requeued sentinel value" id))))

(with-post-hook! #'apply-fn-shim
  (fn [{:keys [onyx.core/id onyx.core/results]}]
    (debug (format "[%s] Applied fn to %s segments" id (count results)))))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/compressed]}]
    (debug (format "[%s] Compressed %s segments" id (count compressed)))))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [onyx.core/id onyx.core/producers]}]
    (debug (format "[%s] Wrote batch to %s outputs" id (count producers)))))

(with-post-hook! #'seal-resource-shim
  (fn [{:keys [onyx.core/id]}]
    (debug (format "[%s] Sealing resource" id))))

