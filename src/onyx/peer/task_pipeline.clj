(ns onyx.peer.task-pipeline
  (:require [clojure.core.async :refer [alts!! <!! >!! >! chan close! go thread]]
            [com.stuartsierra.component :as component]
            [dire.core :as dire]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.queue.hornetq :refer [hornetq]]
            [onyx.peer.transform :as transform]
            [onyx.extensions :as extensions]))

(defn create-tx-session [{:keys [queue]}]
  (extensions/create-tx-session queue))

(defn new-payload [sync peer-node payload-ch]
  (let [peer-contents (extensions/read-place sync peer-node)
        node (extensions/create sync :payload)
        updated-contents (assoc peer-contents :payload node)]
    (extensions/write-place sync peer-node updated-contents)
    node))

(defn munge-new-payload [{:keys [sync peer-node payload-ch] :as event}]
  (let [node (new-payload sync peer-node payload-ch)]
    (extensions/on-change sync node #(>!! payload-ch %))
    (assoc event :new-payload-node node)))

(defn munge-open-session [event session]
  (assoc event :session session))

(defn munge-read-batch [event]
  (let [rets (p-ext/read-batch event)]
    (merge event rets)))

(defn munge-decompress-batch [event]
  (let [rets (p-ext/decompress-batch event)]
    (merge event rets)))

(defn munge-apply-fn [event]
  (let [rets (p-ext/apply-fn event)]
    (merge event rets)))

(defn munge-compress-batch [event]
  (let [rets (p-ext/compress-batch event)]
    (merge event rets)))

(defn munge-write-batch [event]
  (let [rets (p-ext/write-batch event)]
    (merge event rets)))

(defn munge-status-check [{:keys [sync status-node] :as event}]
  (assoc event :commit? (extensions/place-exists? sync status-node)))

(defn munge-commit-tx [{:keys [queue session] :as event}]
  (extensions/commit-tx queue session)
  (assoc event :committed true))

(defn munge-close-resources [{:keys [queue session producers consumers] :as event}]
  (doseq [producer producers] (extensions/close-resource queue producer))
  (doseq [consumer consumers] (extensions/close-resource queue consumer))
  (extensions/close-resource queue session)
  (assoc event :closed true))

(defn munge-complete-task [{:keys [sync completion-node decompressed] :as event}]
  (let [done? (= (last decompressed) :done)]
    (when done?
      (extensions/touch-place sync completion-node))
    (assoc event :completed done?)))

(defn open-session-loop [read-ch pipeline-data]
  (loop []
    (when-let [session (create-tx-session pipeline-data)]
      (>!! read-ch (munge-open-session pipeline-data session))
      (recur))))

(defn read-batch-loop [read-ch decompress-ch]
  (loop []
    (when-let [event (<!! read-ch)]
      (>!! decompress-ch (munge-read-batch event))
      (recur))))

(defn decompress-batch-loop [decompress-ch apply-fn-ch]
  (loop []
    (when-let [event (<!! decompress-ch)]
      (>!! apply-fn-ch (munge-decompress-batch event))
      (recur))))

(defn apply-fn-loop [apply-fn-ch compress-ch]
  (loop []
    (when-let [event (<!! apply-fn-ch)]
      (>!! compress-ch (munge-apply-fn event))
      (recur))))

(defn compress-batch-loop [compress-ch write-batch-ch]
  (loop []
    (when-let [event (<!! compress-ch)]
      (>!! write-batch-ch (munge-compress-batch event))
      (recur))))

(defn write-batch-loop [write-ch status-check-ch]
  (loop []
    (when-let [event (<!! write-ch)]
      (>!! status-check-ch (munge-write-batch event))
      (recur))))

(defn status-check-loop [status-ch commit-tx-ch reset-payload-node-ch]
  (loop []
    (when-let [event (<!! status-ch)]
      (let [event (munge-status-check event)]
        (if (:commit? event)
          (>!! commit-tx-ch event)
          (>!! reset-payload-node-ch event))
        (recur)))))

(defn commit-tx-loop [commit-ch close-resources-ch]
  (loop []
    (when-let [event (<!! commit-ch)]
      (>!! close-resources-ch (munge-commit-tx event))
      (recur))))

(defn close-resources-loop [close-ch complete-task-ch]
  (loop []
    (when-let [event (<!! close-ch)]
      (>!! complete-task-ch (munge-close-resources event))
      (recur))))

(defn complete-task-loop [complete-ch reset-payload-node-ch]
  (loop []
    (when-let [event (<!! complete-ch)]
      (let [event (munge-complete-task event)]
        (when (:completed event)
          (>!! reset-payload-node-ch event)))
      (recur))))

(defn reset-payload-node [reset-ch]
  (when-let [event (<!! reset-ch)]
    (munge-new-payload event)
    (>!! (:complete-ch event) true)))

(defrecord TaskPipeline [payload sync queue payload-ch complete-ch]
  component/Lifecycle

  (start [component]
    (prn "Starting Task Pipeline")
    
    (let [read-batch-ch (chan 1)
          decompress-batch-ch (chan 1)
          apply-fn-ch (chan 1)
          compress-batch-ch (chan 1)
          status-check-ch (chan 1)
          write-batch-ch (chan 1)
          commit-tx-ch (chan 1)
          close-resources-ch (chan 1)
          complete-task-ch (chan 1)
          reset-payload-node-ch (chan 1)

          pipeline-data {:ingress-queues (:task/ingress-queues (:task payload))
                         :egress-queues (:task/egress-queues (:task payload))
                         :task (:task/name (:task payload))
                         :peer-node (:peer (:nodes payload))
                         :status-node (:status (:nodes payload))
                         :completion-node (:completion (:nodes payload))
                         :catalog (read-string (extensions/read-place sync (:catalog (:nodes payload))))
                         :workflow (read-string (extensions/read-place sync (:workflow (:nodes payload))))
                         :payload-ch payload-ch
                         :complete-ch complete-ch
                         :queue queue
                         :sync sync
                         :batch-size 2
                         :timeout 50}]

      (dire/with-handler! #'open-session-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'read-batch-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'decompress-batch-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'apply-fn-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'compress-batch-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'write-batch-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'status-check-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'commit-tx-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'close-resources-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'complete-task-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'reset-payload-node
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (assoc component
        :read-batch-ch read-batch-ch
        :decompress-batch-ch decompress-batch-ch
        :apply-fn-ch apply-fn-ch
        :compress-batch-ch compress-batch-ch
        :write-batch-ch write-batch-ch
        :status-check-ch status-check-ch
        :commit-tx-ch commit-tx-ch
        :close-resources-ch close-resources-ch
        :complete-task-ch complete-task-ch
        :reset-payload-node-ch reset-payload-node-ch        
        
        :open-session-loop (future (open-session-loop read-batch-ch pipeline-data))
        :read-batch-loop (thread (read-batch-loop read-batch-ch decompress-batch-ch))
        :decompress-batch-loop (thread (decompress-batch-loop decompress-batch-ch apply-fn-ch))
        :apply-fn-loop (thread (apply-fn-loop apply-fn-ch compress-batch-ch))
        :compress-batch-loop (thread (compress-batch-loop compress-batch-ch write-batch-ch))
        :write-batch-loop (thread (write-batch-loop write-batch-ch status-check-ch))
        :status-check-loop (thread (status-check-loop status-check-ch commit-tx-ch reset-payload-node-ch))
        :commit-tx-loop (thread (commit-tx-loop commit-tx-ch close-resources-ch))
        :close-resources-loop (thread (close-resources-loop close-resources-ch complete-task-ch))
        :complete-task-loop (thread (complete-task-loop complete-task-ch reset-payload-node-ch))
        :reset-payload-node (thread (reset-payload-node reset-payload-node-ch)))))

  (stop [component]
    (prn "Stopping Task Pipeline")

    (future-cancel (:open-session-loop component))

    (close! (:read-batch-ch component))
    (close! (:decompress-batch-ch component))
    (close! (:apply-fn-ch component))
    (close! (:compress-batch-ch component))
    (close! (:write-batch-ch component))
    (close! (:status-check-ch component))
    (close! (:commit-tx-ch component))
    (close! (:close-resources-ch component))
    (close! (:complete-task-ch component))
    (close! (:reset-payload-node-ch component))

    component))

(defn task-pipeline [payload sync queue payload-ch complete-ch]
  (map->TaskPipeline {:payload payload :sync sync
                      :queue queue :payload-ch payload-ch
                      :complete-ch complete-ch}))

