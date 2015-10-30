(ns onyx.static.default-vals)

(def default-pending-timeout 60000)

(def defaults
  {;; input task defaults
   :onyx/input-retry-timeout 1000
   :onyx/pending-timeout default-pending-timeout
   :onyx/max-pending 10000

   ;; task defaults
   :onyx/batch-timeout 1000

   ;; zookeeper defaults
   :onyx.zookeeper/backoff-base-sleep-time-ms 1000
   :onyx.zookeeper/backoff-max-sleep-time-ms 30000
   :onyx.zookeeper/backoff-max-retries 5

   ;; bookkeeper defaults
   :onyx.bookkeeper/server? false
   :onyx.bookkeeper/port 3196
   :onyx.bookkeeper/local-quorum? false
   :onyx.bookkeeper/local-quorum-ports [3196 3197 3198]
   :onyx.bookkeeper/base-journal-dir "/tmp/bookkeeper_journal"
   :onyx.bookkeeper/base-ledger-dir "/tmp/bookkeeper_ledger"
   :onyx.bookkeeper/client-timeout 60000 
   :onyx.bookkeeper/client-throttle 30000
   :onyx.bookkeeper/ledger-password "INSECUREDEFAULTPASSWORD" 
   :onyx.bookkeeper/ledger-ensemble-size 3
   :onyx.bookkeeper/ledger-quorum-size 3
   :onyx.bookkeeper/ledger-id-written-back-off 50
   ; number of state entries to write to a single bookeeper entry
   :onyx.bookkeeper/write-batch-size 20
   :onyx.bookkeeper/write-buffer-size 10000
   :onyx.bookkeeper/write-batch-timeout 50
   ; number of bookkeeper entries to read at a time
   ; effective batch read of state entries is write-batch-size * read-batch-size
   :onyx.bookkeeper/read-batch-size 50

   :onyx.rocksdb.filter/base-dir "/tmp/rocksdb_filter"
   :onyx.rocksdb.filter/bloom-filter-bits 10
   :onyx.rocksdb.filter/compression :none
   :onyx.rocksdb.filter/block-size 4096
   ; rocksdb cache per filtering peer, 100MB
   :onyx.rocksdb.filter/peer-block-cache-size (* 100 1024 1024)
   ; number of filter buckets
   :onyx.rocksdb.filter/num-buckets 10
   ; rotate the filter bucket every n elements
   :onyx.rocksdb.filter/num-ids-per-bucket 10000000

   ;; peer defaults
   :onyx.peer/inbox-capacity 1000
   :onyx.peer/outbox-capacity 1000
   :onyx.peer/drained-back-off 400
   :onyx.peer/job-not-ready-back-off 500
   :onyx.peer/peer-not-ready-back-off 500
   :onyx.peer/backpressure-check-interval 10
   :onyx.peer/backpressure-low-water-pct 30
   :onyx.peer/backpressure-high-water-pct 60
   :onyx.peer/state-log-impl :bookkeeper
   :onyx.peer/state-filter-impl :rocksdb

   ;; messaging defaults
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging.aeron/offer-idle-strategy :high-restart-latency
   :onyx.messaging.aeron/poll-idle-strategy :high-restart-latency
   :onyx.messaging.aeron/subscriber-count 2
   :onyx.messaging.aeron/write-buffer-size 1000
   :onyx.messaging/allow-short-circuit? true
   :onyx.messaging/inbound-buffer-size 200000
   :onyx.messaging/completion-buffer-size 50000
   :onyx.messaging/release-ch-buffer-size 10000
   :onyx.messaging/retry-ch-buffer-size 10000
   :onyx.messaging/peer-link-gc-interval 90000
   :onyx.messaging/peer-link-idle-timeout 60000
   :onyx.messaging/ack-daemon-timeout default-pending-timeout
   :onyx.messaging/ack-daemon-clear-interval 15000

   ;; windowing defaults
   :onyx.windowing/min-value 0})

(defn arg-or-default [k opts]
  (get opts k (get defaults k)))
