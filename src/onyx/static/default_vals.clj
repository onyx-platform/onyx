(ns onyx.static.default-vals)

(def defaults
  {:onyx/input-retry-timeout 1000
   :onyx/pending-timeout 60000
   :onyx/batch-timeout 1000
   :onyx/max-pending 10000

   :onyx.peer/inbox-capacity 1000
   :onyx.peer/outbox-capacity 1000
   :onyx.peer/drained-back-off 400
   :onyx.peer/sequential-back-off 2000
   :onyx.peer/job-not-ready-back-off 500

   :onyx.messaging/completion-buffer-size 50000
   :onyx.messaging/release-ch-buffer-size 10000
   :onyx.messaging/retry-ch-buffer-size 10000})
