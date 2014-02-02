(ns onyx.peer.task-pipeline
  (:require [clojure.core.async :refer [<!! >!! chan close!]]
            [com.stuartsierra.component :as component]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.extensions :as extensions]))

(defn open-session-loop [queue read-ch]
  (loop []
    (recur)))

(defn read-batch-loop [read-ch decompress-ch]
  (loop []
    (when-let [event (<!! read-ch)]
      (recur))))

(defn decompress-tx-loop [decompress-ch apply-fn-ch]
  (loop []
    (when-let [event (<!! decompress-ch)]
      (recur))))

(defn apply-fn-loop [apply-fn-ch compress-ch]
  (loop []
    (when-let [event (<!! apply-fn-ch)]
      (recur))))

(defn compress-tx-loop [compress-ch write-batch-ch]
  (loop []
    (when-let [event (<!! compress-ch)]
      (recur))))

(defn write-batch-loop [write-ch status-check-ch]
  (loop []
    (when-let [event (<!! write-ch)]
      (recur))))

(defn status-check-loop [status-ch commit-tx-ch]
  (loop []
    (when-let [event (<!! status-ch)]
      (recur))))

(defn commit-tx-loop [commit-ch close-session-ch]
  (loop []
    (when-let [event (<!! commit-ch)]
      (recur))))

(defn close-session-loop [close-ch complete-task-ch]
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
          close-session-ch (chan 1)
          complete-task-ch (chan 1)
          reset-payload-node-ch (chan 1)]

      (assoc component
        :read-batch-ch read-batch-ch
        :decompress-tx-ch decompress-tx-ch
        :apply-fn-ch apply-fn-ch
        :compress-tx-ch compress-tx-ch
        :write-batch-ch write-batch-ch
        :status-check-ch status-check-ch
        :commit-tx-ch commit-tx-ch
        :close-session-ch close-session-ch
        :complete-task-ch complete-task-ch
        :reset-payload-node-ch reset-payload-node-ch        
        
        :open-session-loop (future (open-session-loop queue read-batch-ch))
        :read-batch-loop (future (read-batch-loop read-batch-ch decompress-tx-ch))
        :decompress-tx-loop (future (decompress-tx-loop decompress-tx-ch apply-fn-ch))
        :apply-fn-loop (future (apply-fn-loop apply-fn-ch compress-tx-ch))
        :compress-tx-loop (future (compress-tx-loop compress-tx-ch write-batch-ch))
        :write-batch-loop (future (write-batch-loop write-batch-ch status-check-ch))
        :status-check-loop (future (status-check-loop status-check-ch commit-tx-ch))
        :commit-tx-loop (future (commit-tx-loop commit-tx-ch close-session-ch))
        :close-session-loop (future (close-session-loop close-session-ch complete-task-ch))
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
    (close! (:close-session-ch component))
    (close! (:complete-task-ch component))
    (close! (:reset-payload-node-ch component))

    (future-cancel (:open-session-loop component))
    (future-cancel (:read-batch-loop component))
    (future-cancel (:decompress-tx-loop component))
    (future-cancel (:apply-fn-loop component))
    (future-cancel (:compress-tx-loop component))
    (future-cancel (:status-check-loop component))
    (future-cancel (:write-batch-loop component))
    (future-cancel (:close-session-loop component))
    (future-cancel (:complete-task-loop component))
    (future-cancel (:reset-payload-node-loop component))
    
    component))

(defn task-pipeline []
  (map->TaskPipeline {}))

