(ns onyx.peer.task-pipeline
  (:require [clojure.core.async :refer [<!! >!! chan close!]]
            [com.stuartsierra.component :as component]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.extensions :as extensions]))

(defn create-tx-session [queue]
  (extensions/create-tx-session queue))

(defn read-batch [queue consumer batch-size]
  (map (partial extensions/consume-message queue consumer) (range batch-size)))

(defn decompress-message [message]
  (read-string message))

(defn apply-fn [task data]
  (let [user-ns (symbol (name (namespace (:onyx/fn task))))
        user-fn (symbol (name (:onyx/fn task)))]
    ((ns-resolve user-ns user-fn) data)))

(defn compress-msg [message]
  (pr-str message))

(defn write-batch [queue session producer msgs]
  (doseq [msg msgs]
    (extensions/produce-message queue producer msg)))

(defn munge-open-session [session]
  {:session session})

(defn munge-read-batch [{:keys [session] :as event} queue queue-name batch-size]
  (let [consumer (extensions/create-consumer queue session queue-name)
        batch (read-batch queue consumer batch-size)]
    (assoc event :batch batch :consumer consumer)))

(defn munge-decompress-tx [{:keys [batch] :as event}]
  (let [decompressed-msgs (map decompress-message batch)]
    (assoc event :decompressed decompressed-msgs)))

(defn munge-apply-fn [{:keys [decompressed] :as event} task]
  (let [results (map (partial apply-fn task) decompressed)]
    (assoc event :results results)))

(defn munge-compress-tx [{:keys [results] :as event}]
  (let [compressed-msgs (map compress-msg results)]
    (assoc event :compressed compressed-msgs)))

(defn munge-write-batch [{:keys [session compressed] :as event} queue queue-name]
  (let [producer (extensions/create-producer queue session queue-name)
        batch (write-batch queue session producer compressed)]
    (assoc event :producer producer)))

(defn munge-status-check [event sync status-node]
  (assoc event :commit? (extensions/place-exists? status-node)))

(defn munge-commit-tx [{:keys [session] :as event} queue]
  (extensions/commit-tx queue session)
  (assoc event :committed true))

(defn munge-close-resources [{:keys [session producer consumer] :as event} queue]
  (extensions/close-resource queue)
  (assoc event :closed true))

(defn munge-complete-task [event sync complete-node]
  (extensions/touch-place sync complete-node)
  (assoc event :completed true))

(defn open-session-loop [queue read-ch]
  (loop []
    (when-let [session (create-tx-session queue)]
      (>!! read-ch (munge-open-session session))
      (recur))))

(defn read-batch-loop [queue queue-name read-ch decompress-ch batch-size]
  (loop []
    (when-let [event (<!! read-ch)]
      (>!! decompress-ch (munge-read-batch event queue queue-name batch-size))
      (recur))))

(defn decompress-tx-loop [decompress-ch apply-fn-ch]
  (loop []
    (when-let [event (<!! decompress-ch)]
      (>!! apply-fn-ch (munge-decompress-tx event))
      (recur))))

(defn apply-fn-loop [apply-fn-ch compress-ch task]
  (loop []
    (when-let [event (<!! apply-fn-ch)]
      (>!! compress-ch (munge-apply-fn event task))
      (recur))))

(defn compress-tx-loop [compress-ch write-batch-ch]
  (loop []
    (when-let [event (<!! compress-ch)]
      (>!! write-batch-ch (munge-compress-tx event))
      (recur))))

(defn write-batch-loop [queue queue-name write-ch status-check-ch]
  (loop []
    (when-let [event (<!! write-ch)]
      (>!! status-check-ch (munge-write-batch event queue queue-name))
      (recur))))

(defn status-check-loop [sync status-node status-ch commit-tx-ch reset-payload-node-ch]
  (loop []
    (when-let [event (<!! status-ch)]
      (let [event (munge-status-check event sync)]
        (if (:commit? event)
          (>!! commit-tx-ch event)
          (>!! reset-payload-node-ch event))
        (recur)))))

(defn commit-tx-loop [queue commit-ch close-resources-ch]
  (loop []
    (when-let [event (<!! commit-ch)]
      (>!! close-resources-ch (munge-commit-tx event queue))
      (recur))))

(defn close-resources-loop [queue close-ch complete-task-ch]
  (loop []
    (when-let [event (<!! close-ch)]
      (>!! complete-task-ch (munge-close-resources event queue))
      (recur))))

(defn complete-task-loop [sync complete-ch reset-payload-node-ch completed-node]
  (loop []
    (when-let [event (<!! complete-ch)]
      (>!! reset-payload-node-ch (munge-complete-task event sync completed-node))
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

          batch-size 1000

          queue-name "???"
          status-node "???"
          completed-node "???"]

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
        :read-batch-loop (future (read-batch-loop read-batch-ch queue-name decompress-tx-ch batch-size))
        :decompress-tx-loop (future (decompress-tx-loop decompress-tx-ch apply-fn-ch))
        :apply-fn-loop (future (apply-fn-loop apply-fn-ch compress-tx-ch))
        :compress-tx-loop (future (compress-tx-loop compress-tx-ch write-batch-ch))
        :write-batch-loop (future (write-batch-loop queue queue-name write-batch-ch status-check-ch))
        :status-check-loop (future (status-check-loop sync status-node status-check-ch commit-tx-ch reset-payload-node-ch))
        :commit-tx-loop (future (commit-tx-loop queue commit-tx-ch close-resources-ch))
        :close-resources-loop (future (close-resources-loop queue close-resources-ch complete-task-ch))
        :complete-task-loop (future (complete-task-loop sync complete-task-ch reset-payload-node-ch completed-node))
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

