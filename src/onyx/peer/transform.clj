(ns onyx.peer.transform
  (:require [clojure.core.async :refer [chan go alts!! close! >!] :as async]
            [clojure.data.fressian :as fressian]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.coordinator.planning :refer [find-task]]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info]]
            [dire.core :refer [with-post-hook!]]))

(defn read-batch [queue consumers catalog task-name]
  ;; Multi-consumer not yet implemented.
  (let [task (find-task catalog task-name)
        consumer (first consumers)
        f #(when-let [m (extensions/consume-message queue consumer (:onyx/timeout task))]
             m)
        rets (doall (repeatedly (:onyx/batch-size task) f))]
    (filter identity rets)))

(defn decompress-segment [queue message]
  (extensions/read-message queue message))

(defn apply-fn [task segment]
  (let [user-ns (symbol (name (namespace (:onyx/fn task))))
        user-fn (symbol (name (:onyx/fn task)))]
    ((ns-resolve user-ns user-fn) segment)))

(defn compress-segment [segment]
  (.array (fressian/write segment)))

(defn write-batch [queue session producers msgs]
  (dorun
   (for [p producers msg msgs]
     (extensions/produce-message queue p session msg)))
  {:written? true})

(defn read-batch-shim
  [{:keys [queue session ingress-queues catalog task]}]
  (let [consumers (map (partial extensions/create-consumer queue session) ingress-queues)
        batch (read-batch queue consumers catalog task)]
    {:batch batch :consumers consumers}))

(defn decompress-batch-shim [{:keys [queue batch]}]
  (let [decompressed-msgs (map (partial decompress-segment queue) batch)]
    {:decompressed decompressed-msgs}))

(defn acknowledge-batch-shim [{:keys [queue batch]}]
  (doseq [message batch]
    (extensions/ack-message queue message))
  {:acked (count batch)})

(defn apply-fn-shim [{:keys [decompressed task catalog]}]
  (let [task (first (filter (fn [entry] (= (:onyx/name entry) task)) catalog))
        results (map (partial apply-fn task) decompressed)]
    {:results results}))

(defn compress-batch-shim [{:keys [results]}]
  (let [compressed-msgs (map compress-segment results)]
    {:compressed compressed-msgs}))

(defn write-batch-shim [{:keys [queue egress-queues session compressed]}]
  (let [producers (map (partial extensions/create-producer queue session) egress-queues)
        batch (write-batch queue session producers compressed)]
    {:producers producers}))

(defmethod p-ext/read-batch :default
  [event] (read-batch-shim event))

(defmethod p-ext/decompress-batch :default
  [event] (decompress-batch-shim event))

(defmethod p-ext/ack-batch :default
  [event] (acknowledge-batch-shim event))

(defmethod p-ext/apply-fn :default
  [event] (apply-fn-shim event))

(defmethod p-ext/compress-batch :default
  [event] (compress-batch-shim event))

(defmethod p-ext/write-batch :default
  [event] (write-batch-shim event))

(with-post-hook! #'read-batch-shim
  (fn [{:keys [batch consumers]}]
    (info "Transformer: Read batch of" (count batch) "segments")))

(with-post-hook! #'decompress-batch-shim
  (fn [{:keys [decompressed]}]
    (info "Transformer: Decompressed" (count decompressed) "segments")))

(with-post-hook! #'acknowledge-batch-shim
  (fn [{:keys [acked]}]
    (info "Transformer: Acked" acked "segments")))

(with-post-hook! #'apply-fn-shim
  (fn [{:keys [results]}]
    (info "Transformer: Applied fn to" (count results) "segments")))

(with-post-hook! #'compress-batch-shim
  (fn [{:keys [compressed]}]
    (info "Transformer: Compressed" (count compressed) "segments")))

(with-post-hook! #'write-batch-shim
  (fn [{:keys [producers]}]
    (info "Transformer: Wrote batch to" (count producers) "outputs")))

