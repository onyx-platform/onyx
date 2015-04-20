(ns onyx.static.default-vals)

(def defaults
  {:onyx/replay-interval 1000
   :onyx/pending-timeout 60000
   :onyx/batch-timeout 1000
   :onyx/max-pending 10000

   :onyx.peer/inbox-capacity 1000
   :onyx.peer/outbox-capacity 1000
   :onyx.peer/drained-back-off 400
   :onyx.peer/sequential-back-off 2000
   :onyx.peer/job-not-ready-back-off 500})