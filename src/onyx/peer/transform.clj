(ns onyx.peer.transform
  (:require [clojure.core.async :refer [chan go alts!! close! >!] :as async]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]))

(defn read-batch [queue consumers batch-size timeout]
  (let [consumer-chs (take (count consumers) (repeatedly #(chan 1)))]
    (doseq [[c consumer-ch] (map vector consumers consumer-chs)]
      (go (loop []
            (when-let [m (.receive c)]
              (extensions/ack-message queue m)
              (>! consumer-ch m)
              (recur)))))
    (let [chs (conj consumer-chs (async/timeout timeout))
          rets (doall (repeatedly batch-size #(first (alts!! chs))))]
      (doseq [ch chs] (close! ch))
      (filter identity rets))))

(defn decompress-segment [queue message]
  (let [segment (extensions/read-message queue message)]
    (read-string segment)))

(defn apply-fn [task segment]
  (let [user-ns (symbol (name (namespace (:onyx/fn task))))
        user-fn (symbol (name (:onyx/fn task)))]
    ((ns-resolve user-ns user-fn) segment)))

(defn compress-segment [segment]
  (pr-str segment))

(defn write-batch [queue session producers msgs]
  (for [p producers msg msgs]
    (extensions/produce-message queue p session msg)))

(defmethod p-ext/read-batch :default
  [{:keys [task queue session ingress-queues batch-size timeout]}]
  (let [consumers (map (partial extensions/create-consumer queue session) ingress-queues)
        batch (read-batch queue consumers batch-size timeout)]
    {:batch batch :consumers consumers}))

(defmethod p-ext/decompress-batch :default
  [{:keys [queue batch]}]
  (let [decompressed-msgs (map (partial decompress-segment queue) batch)]
    {:decompressed decompressed-msgs}))

(defmethod p-ext/apply-fn :default
  [{:keys [decompressed task catalog]}]
  (let [task (first (filter (fn [entry] (= (:onyx/name entry) task)) catalog))
        results (map (partial apply-fn task) decompressed)]
    {:results results}))

(defmethod p-ext/compress-batch :default
  [{:keys [results]}]
  (let [compressed-msgs (map compress-segment results)]
    {:compressed compressed-msgs}))

(defmethod p-ext/write-batch :default
  [{:keys [queue egress-queues session compressed]}]
  (prn "Transformer: Writing batch " compressed)
  (let [producers (map (partial extensions/create-producer queue session) egress-queues)
        batch (write-batch queue session producers compressed)]
    {:producers producers}))

