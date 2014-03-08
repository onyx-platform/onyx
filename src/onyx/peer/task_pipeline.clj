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

(defn munge-new-payload [{:keys [sync peer-node peer-version payload-ch] :as event}]
  (if (= (extensions/version sync peer-node) peer-version)
    (let [node (new-payload sync peer-node payload-ch)]
      (extensions/on-change sync node #(>!! payload-ch %))
      (assoc event :new-payload-node node :completion? true))
    event))

(defn munge-open-session [event session]
  (assoc event :session session))

(defn munge-read-batch [event]
  (let [rets (p-ext/read-batch event)]
    (merge event rets)))

(defn munge-decompress-batch [event]
  (let [rets (p-ext/decompress-batch event)]
    (merge event rets)))

(defn munge-strip-sentinel [event]
  (let [segments (:decompressed event)]
    (if (= (last segments) :done)
      (assoc event :tail-batch? true :decompressed (or (butlast segments) []))
      (assoc event :tail-batch? false))))

(defn munge-requeue-sentinel [{:keys [queue session ingress-queues tail-batch?] :as event}]
  (when tail-batch?
    ;; Caps all queues - only supports 1 consumers
    (extensions/cap-queue queue ingress-queues))
  event)

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

(defn munge-ack [{:keys [queue batch] :as event}]
  (let [rets (p-ext/ack-batch event)]
    (merge event rets)))

(defn munge-commit-tx [{:keys [queue session] :as event}]
  (extensions/commit-tx queue session)
  (assoc event :committed true))

(defn munge-close-resources [{:keys [queue session producers consumers] :as event}]
  (doseq [producer producers] (extensions/close-resource queue producer))
  (doseq [consumer consumers] (extensions/close-resource queue consumer))
  (extensions/close-resource queue session)
  (assoc event :closed true))

(defn munge-complete-task
  [{:keys [sync completion-node completion?] :as event}]
  (when completion?
    (extensions/touch-place sync completion-node))
  event)

(defn open-session-loop [read-ch kill-ch pipeline-data]
  (loop []
    (when (first (alts!! [kill-ch] :default true))
      (when-let [session (create-tx-session pipeline-data)]
        (>!! read-ch (munge-open-session pipeline-data session))
        (recur)))))

(defn read-batch-loop [read-ch decompress-ch]
  (loop []
    (when-let [event (<!! read-ch)]
      (>!! decompress-ch (munge-read-batch event))
      (recur))))

(defn decompress-batch-loop [decompress-ch strip-ch]
  (loop []
    (when-let [event (<!! decompress-ch)]
      (>!! strip-ch (munge-decompress-batch event))
      (recur))))

(defn strip-sentinel-loop [strip-ch requeue-ch]
  (loop []
    (when-let [event (<!! strip-ch)]
      (>!! requeue-ch (munge-strip-sentinel event))
      (recur))))

(defn requeue-sentinel-loop [requeue-ch apply-fn-ch]
  (loop []
    (when-let [event (<!! requeue-ch)]
      (>!! apply-fn-ch (munge-requeue-sentinel event))
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

(defn status-check-loop [status-ch ack-ch close-resources-ch]
  (loop []
    (when-let [event (<!! status-ch)]
      (let [event (munge-status-check event)]
        (if (:commit? event)
          (>!! ack-ch event)
          (>!! close-resources-ch event))
        (recur)))))

(defn ack-loop [ack-ch commit-ch]
  (loop []
    (when-let [event (<!! ack-ch)]
      (>!! commit-ch (munge-ack event))
      (recur))))

(defn commit-tx-loop [commit-ch close-resources-ch]
  (loop []
    (when-let [event (<!! commit-ch)]
      (>!! close-resources-ch (munge-commit-tx event))
      (recur))))

(defn close-resources-loop [close-ch reset-payload-ch]
  (loop []
    (when-let [event (<!! close-ch)]
      (>!! reset-payload-ch (munge-close-resources event))
      (recur))))

(defn reset-payload-node [reset-ch internal-complete-ch]
  (loop []
    (when-let [event (<!! reset-ch)]
      (if (:tail-batch? event)
        (let [event (munge-new-payload event)]
          (>!! internal-complete-ch event))
        (>!! internal-complete-ch event))
      (recur))))

(defn complete-task-loop [complete-ch]
  (loop []
    (when-let [event (<!! complete-ch)]
      (when (:tail-batch? event)
        (munge-complete-task event)
        (>!! (:complete-ch event) true))
      (recur))))

(defrecord TaskPipeline [payload sync queue payload-ch complete-ch]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Task Pipeline")
    
    (let [open-session-kill-ch (chan 1)
          read-batch-ch (chan 1)
          decompress-batch-ch (chan 1)
          strip-sentinel-ch (chan 1)
          requeue-sentinel-ch (chan 1)
          apply-fn-ch (chan 1)
          compress-batch-ch (chan 1)
          status-check-ch (chan 1)
          write-batch-ch (chan 1)
          ack-ch (chan 1)
          commit-tx-ch (chan 1)
          close-resources-ch (chan 1)
          reset-payload-node-ch (chan 1)
          complete-task-ch (chan 1)

          pipeline-data {:ingress-queues (:task/ingress-queues (:task payload))
                         :egress-queues (:task/egress-queues (:task payload))
                         :task (:task/name (:task payload))
                         :peer-node (:peer (:nodes payload))
                         :status-node (:status (:nodes payload))
                         :completion-node (:completion (:nodes payload))
                         :catalog (read-string (extensions/read-place sync (:catalog (:nodes payload))))
                         :workflow (read-string (extensions/read-place sync (:workflow (:nodes payload))))
                         :peer-version (extensions/version sync (:peer (:nodes payload)))
                         :payload-ch payload-ch
                         :complete-ch complete-ch
                         :queue queue
                         :sync sync}]

      (dire/with-handler! #'open-session-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'read-batch-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'decompress-batch-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'strip-sentinel-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'requeue-sentinel-loop
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

      (dire/with-handler! #'ack-loop
        java.lang.Exception
        (fn [e & args] (.printStackTrace e)))

      (dire/with-handler! #'commit-tx-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'close-resources-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'reset-payload-node
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (dire/with-handler! #'complete-task-loop
        java.lang.Exception
        (fn [e & _] (.printStackTrace e)))

      (assoc component
        :open-session-kill-ch open-session-kill-ch
        :read-batch-ch read-batch-ch
        :decompress-batch-ch decompress-batch-ch
        :strip-sentinel-ch strip-sentinel-ch
        :requeue-sentinel-ch requeue-sentinel-ch
        :apply-fn-ch apply-fn-ch
        :compress-batch-ch compress-batch-ch
        :write-batch-ch write-batch-ch
        :status-check-ch status-check-ch
        :ack-ch ack-ch
        :commit-tx-ch commit-tx-ch
        :close-resources-ch close-resources-ch
        :reset-payload-node-ch reset-payload-node-ch
        :complete-task-ch complete-task-ch
        
        :open-session-loop (thread (open-session-loop read-batch-ch open-session-kill-ch pipeline-data))
        :read-batch-loop (thread (read-batch-loop read-batch-ch decompress-batch-ch))
        :decompress-batch-loop (thread (decompress-batch-loop decompress-batch-ch strip-sentinel-ch))
        :strip-sentinel-loop (thread (strip-sentinel-loop strip-sentinel-ch requeue-sentinel-ch))
        :requeue-sentinel-ch (thread (requeue-sentinel-loop requeue-sentinel-ch apply-fn-ch))
        :apply-fn-loop (thread (apply-fn-loop apply-fn-ch compress-batch-ch))
        :compress-batch-loop (thread (compress-batch-loop compress-batch-ch write-batch-ch))
        :write-batch-loop (thread (write-batch-loop write-batch-ch status-check-ch))
        :status-check-loop (thread (status-check-loop status-check-ch ack-ch close-resources-ch))
        :ack-loop (thread (ack-loop ack-ch commit-tx-ch))
        :commit-tx-loop (thread (commit-tx-loop commit-tx-ch close-resources-ch))
        :close-resources-loop (thread (close-resources-loop close-resources-ch reset-payload-node-ch))
        :reset-payload-node (thread (reset-payload-node reset-payload-node-ch complete-task-ch))
        :complete-task-loop (thread (complete-task-loop complete-task-ch)))))

  (stop [component]
    (taoensso.timbre/info "Stopping Task Pipeline")

    (close! (:open-session-kill-ch component))
    (close! (:read-batch-ch component))
    (close! (:decompress-batch-ch component))
    (close! (:strip-sentinel-ch component))
    (close! (:requeue-sentinel-ch component))
    (close! (:apply-fn-ch component))
    (close! (:compress-batch-ch component))
    (close! (:write-batch-ch component))
    (close! (:status-check-ch component))
    (close! (:ack-ch component))
    (close! (:commit-tx-ch component))
    (close! (:close-resources-ch component))
    (close! (:reset-payload-node-ch component))
    (close! (:complete-task-ch component))

    component))

(defn task-pipeline [payload sync queue payload-ch complete-ch]
  (map->TaskPipeline {:payload payload :sync sync
                      :queue queue :payload-ch payload-ch
                      :complete-ch complete-ch}))

