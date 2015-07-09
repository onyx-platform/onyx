(ns onyx.static.default-vals)

(def default-pending-timeout 60000)

(def defaults
  {; input task defaults
   :onyx/input-retry-timeout 1000
   :onyx/pending-timeout default-pending-timeout
   :onyx/max-pending 10000

   ; task defaults
   :onyx/batch-timeout 1000

   ; zookeeper defaults
   :onyx.zookeeper/backoff-base-sleep-time-ms 1000
   :onyx.zookeeper/backoff-max-sleep-time-ms 30000
   :onyx.zookeeper/backoff-max-retries 5

   ; peer defaults
   :onyx.peer/inbox-capacity 1000
   :onyx.peer/outbox-capacity 1000
   :onyx.peer/drained-back-off 400
   :onyx.peer/job-not-ready-back-off 500
   :onyx.peer/backpressure-check-interval 10
   :onyx.peer/backpressure-low-water-pct 30
   :onyx.peer/backpressure-high-water-pct 60

   ; messaging defaults
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging.aeron/idle-strategy :low-restart-latency
   :onyx.messaging.netty/thread-pool-sizes 1
   :onyx.messaging.netty/connect-timeout-millis 1000
   :onyx.messaging.netty/pending-buffer-size 10000
   :onyx.messaging/allow-short-circuit? true
   :onyx.messaging/inbound-buffer-size 200000
   :onyx.messaging/completion-buffer-size 50000
   :onyx.messaging/release-ch-buffer-size 10000
   :onyx.messaging/retry-ch-buffer-size 10000
   :onyx.messaging/max-downstream-links 10
   :onyx.messaging/max-acker-links 5
   :onyx.messaging/peer-link-gc-interval 90000
   :onyx.messaging/peer-link-idle-timeout 60000
   :onyx.messaging/ack-daemon-timeout default-pending-timeout
   :onyx.messaging/ack-daemon-clear-interval 15000})

(defn arg-or-default [k opts]
  (let [opts-val (get opts k)]
    (if (nil? opts-val)
      (get defaults k)
      opts-val)))
