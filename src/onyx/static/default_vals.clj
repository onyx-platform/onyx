(ns onyx.static.default-vals)

(def defaults
  {; input task defaults
   :onyx/input-retry-timeout 1000
   :onyx/pending-timeout 60000
   :onyx/max-pending 10000

   ; task defaults
   :onyx/batch-timeout 1000

   ; zookeeper defaults
   :onyx.peer/zookeeper-timeout 6000

   ; peer defaults
   :onyx.peer/inbox-capacity 1000
   :onyx.peer/outbox-capacity 1000
   :onyx.peer/drained-back-off 400
   :onyx.peer/job-not-ready-back-off 500

   ; messaging defaults
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging.netty/thread-pool-sizes 1
   :onyx.messaging.netty/connect-timeout-millis 1000
   :onyx.messaging.netty/pending-buffer-size 10000
   :onyx.messaging/completion-buffer-size 50000
   :onyx.messaging/release-ch-buffer-size 10000
   :onyx.messaging/retry-ch-buffer-size 10000
   :onyx.messaging/max-downstream-links 10})
