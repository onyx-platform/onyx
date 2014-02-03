(ns onyx.peer.task-pipeline
  (:require [clojure.core.async :refer [<!! >!! chan close!]]
            [com.stuartsierra.component :as component]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.extensions :as extensions]))

(defn create-tx-session [queue]
  (extensions/create-tx-session queue))

(defn read-batch [queue session queue-name batch-size]
  (let [consumer (extensions/create-consumer queue session queue-name)]
    (map (partial extensions/consume-message consumer) (range batch-size))))

(defn decompress-message [message]
  (read-string message))

(defn apply-fn [task data]
  (let [user-ns (symbol (name (namespace (:onyx/fn task))))
        user-fn (symbol (name (:onyx/fn task)))]
    ((ns-resolve user-ns user-fn) data)))

(defn compress-msg [message]
  (pr-str message))

(defn open-session-loop [queue read-ch]
  (loop []
    (>!! read-ch (create-tx-session queue))
    (recur)))

(defn read-batch-loop [queue queue-name read-ch decompress-ch batch-size]
  (loop []
    (when-let [session (<!! read-ch)]
      (let [batch (read-batch queue session queue-name batch-size)]
        (>!! decompress-ch batch))
      (recur))))

(defn decompress-tx-loop [decompress-ch apply-fn-ch]
  (loop []
    (when-let [batch (<!! decompress-ch)]
      (let [decompressed-msgs (map decompress-message batch)]
        (>!! apply-fn-ch decompressed-msgs))
      (recur))))

(defn apply-fn-loop [apply-fn-ch compress-ch task]
  (loop []
    (when-let [msgs (<!! apply-fn-ch)]
      (let [rets (map (partial apply-fn task) msgs)]
        (>!! compress-ch rets))
      (recur))))

(defn compress-tx-loop [compress-ch write-batch-ch]
  (loop []
    (when-let [batch (<!! compress-ch)]
      (let [compressed-msgs (map compress-msg batch)]
        (>!! compress-ch batch))
      (recur))))

(defn write-batch-loop [write-ch status-check-ch]
  (loop []
    (when-let [event (<!! write-ch)]
      (recur))))

(defn status-check-loop [status-ch commit-tx-ch]
  (loop []
    (when-let [event (<!! status-ch)]
      (recur))))

(defn commit-tx-loop [commit-ch close-resources-ch]
  (loop []
    (when-let [event (<!! commit-ch)]
      (recur))))

(defn close-resources-loop [close-ch complete-task-ch]
  (loop []
    (when-let [event (<!! close-ch)]
      (recur))))

(defn complete-task-loop [complete-ch reset-payload-node-ch]
  (loop []
    (when-let [event (<!! complete-ch)]
      (recur))))

(defn reset-payload-node-loop [reset-ch]
  (loop []
    (when-let [event (<!! reset-ch)]
      (recur))))

(defrecord TaskPipeline [payload-node]
  component/Lifecycle

  (start [{:keys [sync queue] :as component}]
    (prn "Starting Task Pipeline")

    (let [read-batch-ch (chan 1)
          decompress-tx-ch (chan 1)
          apply-fn-ch (chan 1)
          compress-tx-ch (chan 1)
          status-check-ch (chan 1)
          write-batch-ch (chan 1)
          commit-tx-ch (chan 1)
          close-resources-ch (chan 1)
          complete-task-ch (chan 1)
          reset-payload-node-ch (chan 1)

          payload (extensions/read-place sync payload-node)

          batch-size 1000]

      (assoc component
        :read-batch-ch read-batch-ch
        :decompress-tx-ch decompress-tx-ch
        :apply-fn-ch apply-fn-ch
        :compress-tx-ch compress-tx-ch
        :write-batch-ch write-batch-ch
        :status-check-ch status-check-ch
        :commit-tx-ch commit-tx-ch
        :close-resources-ch close-resources-ch
        :complete-task-ch complete-task-ch
        :reset-payload-node-ch reset-payload-node-ch        
        
        :open-session-loop (future (open-session-loop queue read-batch-ch))
        :read-batch-loop (future (read-batch-loop read-batch-ch decompress-tx-ch batch-size))
        :decompress-tx-loop (future (decompress-tx-loop decompress-tx-ch apply-fn-ch))
        :apply-fn-loop (future (apply-fn-loop apply-fn-ch compress-tx-ch))
        :compress-tx-loop (future (compress-tx-loop compress-tx-ch write-batch-ch))
        :write-batch-loop (future (write-batch-loop write-batch-ch status-check-ch))
        :status-check-loop (future (status-check-loop status-check-ch commit-tx-ch))
        :commit-tx-loop (future (commit-tx-loop commit-tx-ch close-resources-ch))
        :close-resources-loop (future (close-resources-loop close-resources-ch complete-task-ch))
        :complete-task-loop (future (complete-task-loop complete-task-ch reset-payload-node-ch))
        :reset-payload-node-loop (future (reset-payload-node-loop reset-payload-node-ch)))))

  (stop [component]
    (prn "Stopping Task Pipeline")

    (close! (:read-batch-ch component))
    (close! (:decompress-tx-ch component))
    (close! (:apply-fn-ch component))
    (close! (:compress-tx-ch component))
    (close! (:write-batch-ch component))
    (close! (:status-check-ch component))
    (close! (:commit-tx-ch component))
    (close! (:close-resources-ch component))
    (close! (:complete-task-ch component))
    (close! (:reset-payload-node-ch component))

    (future-cancel (:open-session-loop component))
    (future-cancel (:read-batch-loop component))
    (future-cancel (:decompress-tx-loop component))
    (future-cancel (:apply-fn-loop component))
    (future-cancel (:compress-tx-loop component))
    (future-cancel (:status-check-loop component))
    (future-cancel (:write-batch-loop component))
    (future-cancel (:close-resources-loop component))
    (future-cancel (:complete-task-loop component))
    (future-cancel (:reset-payload-node-loop component))
    
    component))

(defn task-pipeline []
  (map->TaskPipeline {}))

