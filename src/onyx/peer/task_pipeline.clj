(ns onyx.peer.task-pipeline
  (:require [clojure.core.async :refer [alts!! <!! >!! >! chan close! go thread]]
            [com.stuartsierra.component :as component]
            [dire.core :as dire]
            [taoensso.timbre :as timbre]
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

(defn munge-ack [{:keys [queue batch commit?] :as event}]
  (if commit?
    (let [rets (p-ext/ack-batch event)]
      (merge event rets))
    event))

(defn munge-commit-tx [{:keys [queue session commit?] :as event}]
  (if commit?
    (do (extensions/commit-tx queue session)
        (assoc event :committed true))
    event))

(defn munge-close-resources [{:keys [queue session producers consumers] :as event}]
  (doseq [producer producers] (extensions/close-resource queue producer))
  (doseq [consumer consumers] (extensions/close-resource queue consumer))
  (extensions/close-resource queue session)
  (assoc event :closed? true))

(defn munge-new-payload [{:keys [sync peer-node peer-version payload-ch] :as event}]
  (if (= (extensions/version sync peer-node) peer-version)
    (let [node (new-payload sync peer-node payload-ch)]
      (extensions/on-change sync node #(>!! payload-ch %))
      (assoc event :new-payload-node node :completion? true))
    event))

(defn munge-complete-task
  [{:keys [sync completion-node completion?] :as event}]
  (when completion?
    (extensions/touch-place sync completion-node))
  event)

(defn open-session-loop [read-ch kill-ch pipeline-data dead-ch]
  (loop []
    (when (first (alts!! [kill-ch] :default true))
      (when-let [session (create-tx-session pipeline-data)]
        (>!! read-ch (munge-open-session pipeline-data session))
        (recur))))
  (>!! dead-ch true))

(defn read-batch-loop [read-ch decompress-ch dead-ch]
  (loop []
    (when-let [event (<!! read-ch)]
      (>!! decompress-ch (munge-read-batch event))
      (recur)))
  (>!! dead-ch true))

(defn decompress-batch-loop [decompress-ch strip-ch dead-ch]
  (loop []
    (when-let [event (<!! decompress-ch)]
      (>!! strip-ch (munge-decompress-batch event))
      (recur)))
  (>!! dead-ch true))

(defn strip-sentinel-loop [strip-ch requeue-ch dead-ch]
  (loop []
    (when-let [event (<!! strip-ch)]
      (>!! requeue-ch (munge-strip-sentinel event))
      (recur)))
  (>!! dead-ch true))

(defn requeue-sentinel-loop [requeue-ch apply-fn-ch dead-ch]
  (loop []
    (when-let [event (<!! requeue-ch)]
      (>!! apply-fn-ch (munge-requeue-sentinel event))
      (recur)))
  (>!! dead-ch true))

(defn apply-fn-loop [apply-fn-ch compress-ch dead-ch]
  (loop []
    (when-let [event (<!! apply-fn-ch)]
      (>!! compress-ch (munge-apply-fn event))
      (recur)))
  (>!! dead-ch true))

(defn compress-batch-loop [compress-ch write-batch-ch dead-ch]
  (loop []
    (when-let [event (<!! compress-ch)]
      (>!! write-batch-ch (munge-compress-batch event))
      (recur)))
  (>!! dead-ch true))

(defn write-batch-loop [write-ch status-check-ch dead-ch]
  (loop []
    (when-let [event (<!! write-ch)]
      (>!! status-check-ch (munge-write-batch event))
      (recur)))
  (>!! dead-ch true))

(defn status-check-loop [status-ch ack-ch dead-ch]
  (loop []
    (when-let [event (<!! status-ch)]
      (>!! ack-ch (munge-status-check event))
      (recur)))
  (>!! dead-ch true))

(defn ack-loop [ack-ch commit-ch dead-ch]
  (loop []
    (when-let [event (<!! ack-ch)]
      (>!! commit-ch (munge-ack event))
      (recur)))
  (>!! dead-ch true))

(defn commit-tx-loop [commit-ch close-resources-ch dead-ch]
  (loop []
    (when-let [event (<!! commit-ch)]
      (>!! close-resources-ch (munge-commit-tx event))
      (recur)))
  (>!! dead-ch true))

(defn close-resources-loop [close-ch reset-payload-ch dead-ch]
  (loop []
    (when-let [event (<!! close-ch)]
      (>!! reset-payload-ch (munge-close-resources event))
      (recur)))
  (>!! dead-ch true))

(defn reset-payload-node [reset-ch internal-complete-ch dead-ch]
  (loop []
    (when-let [event (<!! reset-ch)]
      (if (and (:tail-batch? event) (:commit? event))
        (let [event (munge-new-payload event)]
          (>!! internal-complete-ch event))
        (>!! internal-complete-ch event))
      (recur)))
  (>!! dead-ch true))

(defn complete-task-loop [complete-ch dead-ch]
  (loop []
    (when-let [event (<!! complete-ch)]
      (when (and (:tail-batch? event) (:commit? event))
        (munge-complete-task event)
        (when (:completion? event)
          (>!! (:complete-ch event) :task-completed)))
      (recur)))
  (>!! dead-ch true))

(defrecord TaskPipeline [payload sync queue payload-ch complete-ch]
  component/Lifecycle

  (start [component]
    (taoensso.timbre/info "Starting Task Pipeline for" (:task/name (:task payload)))

    (let [open-session-kill-ch (chan 0)
          read-batch-ch (chan 0)
          decompress-batch-ch (chan 0)
          strip-sentinel-ch (chan 0)
          requeue-sentinel-ch (chan 0)
          apply-fn-ch (chan 0)
          compress-batch-ch (chan 0)
          status-check-ch (chan 0)
          write-batch-ch (chan 0)
          ack-ch (chan 0)
          commit-tx-ch (chan 0)
          close-resources-ch (chan 0)
          reset-payload-node-ch (chan 0)
          complete-task-ch (chan 0)

          open-session-dead-ch (chan)
          read-batch-dead-ch (chan)
          decompress-batch-dead-ch (chan)
          strip-sentinel-dead-ch (chan)
          requeue-sentinel-dead-ch (chan)
          apply-fn-dead-ch (chan)
          compress-batch-dead-ch (chan)
          status-check-dead-ch (chan)
          write-batch-dead-ch (chan)
          ack-dead-ch (chan)
          commit-tx-dead-ch (chan)
          close-resources-dead-ch (chan)
          reset-payload-node-dead-ch (chan)
          complete-task-dead-ch (chan)

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
        (fn [e & _] (.printStackTrace e)))

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

        :open-session-dead-ch open-session-dead-ch
        :read-batch-dead-ch read-batch-dead-ch
        :decompress-batch-dead-ch decompress-batch-dead-ch
        :strip-sentinel-dead-ch strip-sentinel-dead-ch
        :requeue-sentinel-dead-ch requeue-sentinel-dead-ch
        :apply-fn-dead-ch apply-fn-dead-ch
        :compress-batch-dead-ch compress-batch-dead-ch
        :status-check-dead-ch status-check-dead-ch
        :write-batch-dead-ch write-batch-dead-ch
        :ack-dead-ch ack-dead-ch
        :commit-tx-dead-ch commit-tx-dead-ch
        :close-resources-dead-ch close-resources-dead-ch
        :reset-payload-node-dead-ch reset-payload-node-dead-ch
        :complete-task-dead-ch complete-task-dead-ch
        
        :open-session-loop (thread (open-session-loop read-batch-ch open-session-kill-ch pipeline-data open-session-dead-ch))
        :read-batch-loop (thread (read-batch-loop read-batch-ch decompress-batch-ch read-batch-dead-ch))
        :decompress-batch-loop (thread (decompress-batch-loop decompress-batch-ch strip-sentinel-ch decompress-batch-dead-ch))
        :strip-sentinel-loop (thread (strip-sentinel-loop strip-sentinel-ch requeue-sentinel-ch strip-sentinel-dead-ch))
        :requeue-sentinel-ch (thread (requeue-sentinel-loop requeue-sentinel-ch apply-fn-ch requeue-sentinel-dead-ch))
        :apply-fn-loop (thread (apply-fn-loop apply-fn-ch compress-batch-ch apply-fn-dead-ch))
        :compress-batch-loop (thread (compress-batch-loop compress-batch-ch write-batch-ch compress-batch-dead-ch))
        :write-batch-loop (thread (write-batch-loop write-batch-ch status-check-ch write-batch-dead-ch))
        :status-check-loop (thread (status-check-loop status-check-ch ack-ch status-check-dead-ch))
        :ack-loop (thread (ack-loop ack-ch commit-tx-ch ack-dead-ch))
        :commit-tx-loop (thread (commit-tx-loop commit-tx-ch close-resources-ch commit-tx-dead-ch))
        :close-resources-loop (thread (close-resources-loop close-resources-ch reset-payload-node-ch close-resources-dead-ch))
        :reset-payload-node (thread (reset-payload-node reset-payload-node-ch complete-task-ch reset-payload-node-dead-ch))
        :complete-task-loop (thread (complete-task-loop complete-task-ch complete-task-dead-ch)))))

  (stop [component]
    (taoensso.timbre/info "Stopping Task Pipeline")

    (close! (:open-session-kill-ch component))
    (<!! (:open-session-dead-ch component))

    (close! (:read-batch-ch component))
    (<!! (:read-batch-dead-ch component))

    (close! (:decompress-batch-ch component))
    (<!! (:decompress-batch-dead-ch component))

    (close! (:strip-sentinel-ch component))
    (<!! (:strip-sentinel-dead-ch component))

    (close! (:requeue-sentinel-ch component))
    (<!! (:requeue-sentinel-dead-ch component))

    (close! (:apply-fn-ch component))
    (<!! (:apply-fn-dead-ch component))

    (close! (:compress-batch-ch component))
    (<!! (:compress-batch-dead-ch component))

    (close! (:write-batch-ch component))
    (<!! (:write-batch-dead-ch component))

    (close! (:status-check-ch component))
    (<!! (:status-check-dead-ch component))

    (close! (:ack-ch component))
    (<!! (:ack-dead-ch component))

    (close! (:commit-tx-ch component))
    (<!! (:commit-tx-dead-ch component))

    (close! (:close-resources-ch component))
    (<!! (:close-resources-dead-ch component))

    (close! (:reset-payload-node-ch component))
    (<!! (:reset-payload-node-dead-ch component))

    (close! (:complete-task-ch component))
    (<!! (:complete-task-dead-ch component))

    (close! (:open-session-dead-ch component))
    (close! (:read-batch-dead-ch component))
    (close! (:decompress-batch-dead-ch component))
    (close! (:strip-sentinel-dead-ch component))
    (close! (:requeue-sentinel-dead-ch component))
    (close! (:apply-fn-dead-ch component))
    (close! (:compress-batch-dead-ch component))
    (close! (:status-check-dead-ch component))
    (close! (:write-batch-dead-ch component))
    (close! (:ack-dead-ch component))
    (close! (:commit-tx-dead-ch component))
    (close! (:close-resources-dead-ch component))
    (close! (:reset-payload-node-dead-ch component))
    (close! (:complete-task-dead-ch component))

    component))

(defn task-pipeline [payload sync queue payload-ch complete-ch]
  (map->TaskPipeline {:payload payload :sync sync
                      :queue queue :payload-ch payload-ch
                      :complete-ch complete-ch}))

(dire/with-post-hook! #'munge-open-session
  (fn [{:keys []}]
    (taoensso.timbre/info (format "[Pipeline] Created new tx session"))))

(dire/with-post-hook! #'munge-read-batch
  (fn [{:keys [batch]}]
    (taoensso.timbre/info (format "[Pipeline] Read %s segments" (count batch)))))

(dire/with-post-hook! #'munge-strip-sentinel
  (fn [{:keys [decompressed]}]
    (taoensso.timbre/info (format "[Pipeline] Stripped sentinel. %s segments left" (count decompressed)))))

(dire/with-post-hook! #'munge-requeue-sentinel
  (fn [{:keys [tail-batch?]}]
    (taoensso.timbre/info (format "[Pipeline] Requeueing sentinel value: %s" tail-batch?))))

(dire/with-post-hook! #'munge-decompress-batch
  (fn [{:keys [decompressed batch]}]
    (taoensso.timbre/info (format "[Pipeline] Decompressed %s segments" (count decompressed)))))

(dire/with-post-hook! #'munge-apply-fn
  (fn [{:keys [results]}]
    (taoensso.timbre/info (format "[Pipeline] Applied fn to %s segments" (count results)))))

(dire/with-post-hook! #'munge-compress-batch
  (fn [{:keys [compressed]}]
    (taoensso.timbre/info (format "[Pipeline] Compressed %s segments" (count compressed)))))

(dire/with-post-hook! #'munge-write-batch
  (fn [{:keys []}]
    (taoensso.timbre/info (format "[Pipeline] Wrote batch"))))

(dire/with-post-hook! #'munge-status-check
  (fn [{:keys [status-node]}]
    (taoensso.timbre/info (format "[Pipeline] Checked the status node"))))

(dire/with-post-hook! #'munge-ack
  (fn [{:keys [acked]}]
    (taoensso.timbre/info (format "[Pipeline] Acked %s segments" acked))))

(dire/with-post-hook! #'munge-commit-tx
  (fn [{:keys [commit?]}]
    (taoensso.timbre/info (format "[Pipeline] Committed transaction? %s" commit?))))

(dire/with-post-hook! #'munge-close-resources
  (fn [{:keys []}]
    (taoensso.timbre/info (format "[Pipeline] Closed resources"))))

