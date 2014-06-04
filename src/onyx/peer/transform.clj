(ns ^:no-doc onyx.peer.transform
  (:require [clojure.data.fressian :as fressian]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.coordinator.planning :refer [find-task]]
            [onyx.extensions :as extensions]
            [onyx.queue.hornetq :refer [take-segments]]
            [taoensso.timbre :refer [debug]]
            [dire.core :refer [with-post-hook!]]))

(defn cap-queue [queue queue-names]
  (let [session (extensions/create-tx-session queue)]
    (doseq [queue-name queue-names]
      (let [producer (extensions/create-producer queue session queue-name)]
        (extensions/produce-message queue producer session (.array (fressian/write :done)))
        (extensions/close-resource queue producer)))
    (extensions/commit-tx queue session)
    (extensions/close-resource queue session)))

(defn read-batch [queue consumers catalog task-name]
  ;; Multi-consumer not yet implemented.
  (let [task (find-task catalog task-name)
        consumer (first consumers)
        f #(extensions/consume-message queue consumer)]
    (doall (take-segments f (:onyx/batch-size task)))))

(defn decompress-segment [queue message]
  (extensions/read-message queue message))

(defn apply-fn [task params segment]
  (let [user-ns (symbol (name (namespace (:onyx/fn task))))
        user-fn (symbol (name (:onyx/fn task)))]
    ((reduce #(partial %1 %2) (ns-resolve user-ns user-fn) params) segment)))

(defn compress-segment [segment]
  {:compressed (.array (fressian/write segment))
   :group (:group (meta segment))})

(defn write-batch [queue session producers msgs]
  (dorun
   (for [p producers msg msgs]
     (if-let [group (:group msg)]
       (extensions/produce-message queue p session (:compressed msg) group)
       (extensions/produce-message queue p session (:compressed msg)))))
  {:written? true})

(defn read-batch-shim
  [{:keys [queue session ingress-queues catalog task] :as event}]
  (let [consumers (map (partial extensions/create-consumer queue session) ingress-queues)
        batch (read-batch queue consumers catalog task)]
    (merge event {:batch batch :consumers consumers})))

(defn decompress-batch-shim [{:keys [queue batch] :as event}]
  (let [decompressed-msgs (map (partial decompress-segment queue) batch)]
    (merge event {:decompressed decompressed-msgs})))

(defn requeue-sentinel-shim [{:keys [queue ingress-queues] :as event}]
  (cap-queue queue ingress-queues)
  (merge event {:requeued? true}))

(defn acknowledge-batch-shim [{:keys [queue batch] :as event}]
  (doseq [message batch]
    (extensions/ack-message queue message))
  (merge event {:acked (count batch)}))

(defn apply-fn-shim [{:keys [decompressed task catalog params] :as event}]
  (let [task (first (filter (fn [entry] (= (:onyx/name entry) task)) catalog))
        results (flatten (map (partial apply-fn task params) decompressed))]
    (merge event {:results results})))

(defn compress-batch-shim [{:keys [results] :as event}]
  (let [compressed-msgs (map compress-segment results)]
    (merge event {:compressed compressed-msgs})))

(defn write-batch-shim [{:keys [queue egress-queues session compressed] :as event}]
  (let [producers (map (partial extensions/create-producer queue session) egress-queues)
        batch (write-batch queue session producers compressed)]
    (merge event {:producers producers})))

(defn seal-resource-shim [{:keys [queue egress-queues] :as event}]
  (merge event (cap-queue queue egress-queues)))

(defmethod l-ext/read-batch :default
  [event] (read-batch-shim event))

(defmethod l-ext/decompress-batch :default
  [event] (decompress-batch-shim event))

(defmethod l-ext/requeue-sentinel :default
  [event] (requeue-sentinel-shim event))

(defmethod l-ext/ack-batch :default
  [event] (acknowledge-batch-shim event))

(defmethod l-ext/apply-fn :default
  [event] (apply-fn-shim event))

(defmethod l-ext/compress-batch :default
  [event] (compress-batch-shim event))

(defmethod l-ext/write-batch :default
  [event] (write-batch-shim event))

(defmethod l-ext/seal-resource :default
  [event] (seal-resource-shim event))

(with-post-hook! #'read-batch-shim
  (fn [{:keys [id batch consumers]}]
    (debug (format "[%s] Read batch of %s segments" id (count batch)))))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [id decompressed]}]
    (debug (format "[%s] Decompressed %s segments" id (count decompressed)))))

(with-post-hook! #'requeue-sentinel-shim
  (fn [{:keys [id]}]
    (debug (format "[%s] Requeued sentinel value" id))))

(with-post-hook! #'acknowledge-batch-shim
  (fn [{:keys [id acked]}]
    (debug (format "[%s] Acked %s segments" id acked))))

(with-post-hook! #'apply-fn-shim
  (fn [{:keys [id results]}]
    (debug (format "[%s] Applied fn to %s segments" id (count results)))))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [id compressed]}]
    (debug (format "[%s] Compressed %s segments" id (count compressed)))))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [id producers]}]
    (debug (format "[%s] Wrote batch to %s outputs" id (count producers)))))

(with-post-hook! #'seal-resource-shim
  (fn [{:keys [id]}]
    (debug (format "[%s] Sealing resource" id))))

