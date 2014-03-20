(ns onyx.logger
  (:require [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info] :as timbre]
            [dire.core :refer [with-post-hook!]]
            [onyx.peer.task-pipeline :as task-pipeline]
            [onyx.peer.transform :as transform]
            [onyx.queue.hornetq :as hornetq]))

(defrecord Logger []
  component/Lifecycle

  (start [component]
    (info "Starting Logger")

    (timbre/set-config!
     [:appenders :standard-out]
     {:min-level nil :enabled? true :async? false :rate-limit nil
      :fn (fn [{:keys [timestamp error? output message]}]
            (binding [*out* (if error? *err* *out*)]
              (timbre/str-println timestamp message)))})

    (with-post-hook! #'task-pipeline/munge-open-session
      (fn [{:keys []}]
        (info (format "[Pipeline] Created new tx session"))))

    (with-post-hook! #'task-pipeline/munge-read-batch
      (fn [{:keys [batch]}]
        (info (format "[Pipeline] Read %s segments" (count batch)))))

    (with-post-hook! #'task-pipeline/munge-strip-sentinel
      (fn [{:keys [decompressed]}]
        (info (format "[Pipeline] Stripped sentinel. %s segments left" (count decompressed)))))

    (with-post-hook! #'task-pipeline/munge-requeue-sentinel
      (fn [{:keys [tail-batch?]}]
        (info (format "[Pipeline] Requeueing sentinel value: %s" tail-batch?))))

    (with-post-hook! #'task-pipeline/munge-decompress-batch
      (fn [{:keys [decompressed batch]}]
        (info (format "[Pipeline] Decompressed %s segments" (count decompressed)))))

    (with-post-hook! #'task-pipeline/munge-apply-fn
      (fn [{:keys [results]}]
        (info (format "[Pipeline] Applied fn to %s segments" (count results)))))

    (with-post-hook! #'task-pipeline/munge-compress-batch
      (fn [{:keys [compressed]}]
        (info (format "[Pipeline] Compressed %s segments" (count compressed)))))

    (with-post-hook! #'task-pipeline/munge-write-batch
      (fn [{:keys []}]
        (info (format "[Pipeline] Wrote batch"))))

    (with-post-hook! #'task-pipeline/munge-status-check
      (fn [{:keys [status-node]}]
        (info (format "[Pipeline] Checked the status node"))))

    (with-post-hook! #'task-pipeline/munge-ack
      (fn [{:keys [acked]}]
        (info (format "[Pipeline] Acked %s segments" acked))))

    (with-post-hook! #'task-pipeline/munge-commit-tx
      (fn [{:keys [commit?]}]
        (info (format "[Pipeline] Committed transaction? %s" commit?))))

    (with-post-hook! #'task-pipeline/munge-close-resources
      (fn [{:keys []}]
        (info (format "[Pipeline] Closed resources"))))

    (with-post-hook! #'transform/read-batch-shim
      (fn [{:keys [batch consumers]}]
        (info "[Transformer] Read batch of" (count batch) "segments")))

    (with-post-hook! #'transform/decompress-batch-shim
      (fn [{:keys [decompressed]}]
        (info "[Transformer] Decompressed" (count decompressed) "segments")))

    (with-post-hook! #'transform/acknowledge-batch-shim
      (fn [{:keys [acked]}]
        (info "[Transformer] Acked" acked "segments")))

    (with-post-hook! #'transform/apply-fn-shim
      (fn [{:keys [results]}]
        (info "[Transformer] Applied fn to" (count results) "segments")))

    (with-post-hook! #'transform/compress-batch-shim
      (fn [{:keys [compressed]}]
        (info "[Transformer] Compressed" (count compressed) "segments")))

    (with-post-hook! #'transform/write-batch-shim
      (fn [{:keys [producers]}]
        (info "[Transformer] Wrote batch to" (count producers) "outputs")))

    (with-post-hook! #'hornetq/read-batch-shim
      (fn [{:keys [batch]}]
        (info "[HornetQ ingress] Read" (count batch) "segments")))

    (with-post-hook! #'hornetq/decompress-batch-shim
      (fn [{:keys [decompressed]}]
        (info "[HornetQ ingress] Decompressed" (count decompressed) "segments")))

    (with-post-hook! #'hornetq/apply-fn-in-shim
      (fn [{:keys [results]}]
        (info "[HornetQ ingress] Applied fn to" (count results) "segments")))

    (with-post-hook! #'hornetq/ack-batch-shim
      (fn [{:keys [acked]}]
        (info "[HornetQ egress] Acked" acked "segments")))

    (with-post-hook! #'hornetq/apply-fn-out-shim
      (fn [{:keys [results]}]
        (info "[HornetQ egress] Applied fn to" (count results) "segments")))

    (with-post-hook! #'hornetq/compress-batch-shim
      (fn [{:keys [compressed]}]
        (info "[HornetQ egress] Compressed batch of" (count compressed) "segments")))

    (with-post-hook! #'hornetq/write-batch-shim
      (fn [{:keys [written?]}]
        (info "[HornetQ egress] Wrote batch with value" written?)))

    component)

  (stop [component]
    (info "Stopping Virtual Peer")

    component))

(defn logger []
  (map->Logger {}))

