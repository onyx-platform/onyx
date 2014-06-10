(ns onyx.extensions
  "Extension interfaces for internally used queues, logs,
   and distributed coordinators.")

(defmulti mark-peer-born (fn [sync place] (type sync)))

(defmulti mark-peer-dead (fn [sync place] (type sync)))

(defmulti mark-offered (fn [sync task peer nodes] (type sync)))

(defmulti plan-job (fn [sync catalog workflow tasks] (type sync)))

(defmulti ack (fn [sync place] (type sync)))

(defmulti seal-resource? (fn [sync place] (type sync)))

(defmulti revoke-offer (fn [sync ack-place] (type sync)))

(defmulti complete (fn [sync complete-place] (type sync)))

(defmulti next-tasks (fn [sync] (type sync)))

(defmulti nodes (fn [sync peer] (type sync)))

(defmulti node->task (fn [sync node] (type sync)))

(defmulti idle-peers (fn [sync] (type sync)))

(defmulti create (fn [sync bucket] [(type sync) bucket]))

(defmulti create-at (fn [sync bucket subpath] [(type sync) bucket]))

(defmulti delete (fn [sync place] (type sync)))

(defmulti write-place (fn [sync place contents] (type sync)))

(defmulti touch-place (fn [sync place] (type sync)))

(defmulti read-place (fn [sync place] (type sync)))

(defmulti read-place-at (fn [sync bucket subpath] [(type sync) bucket]))

(defmulti deref-place-at (fn [sync bucket subpath] [(type sync) bucket]))

(defmulti place-exists? (fn [sync place] (type sync)))

(defmulti version (fn [sync place] (type sync)))

(defmulti on-change (fn [sync place cb] (type sync)))

(defmulti on-delete (fn [sync place db] (type sync)))

(defmulti create-tx-session (fn [queue] (type queue)))

(defmulti create-consumer (fn [queue session queue-name] (type queue)))

(defmulti create-producer (fn [queue session queue-name] (type queue)))

(defmulti consume-message (fn [queue consumer] (type queue)))

(defmulti read-message (fn [queue message] (type queue)))

(defmulti ack-message (fn [queue message] (type queue)))

(defmulti produce-message (fn ([queue producer session msg] (type queue))
                            ([queue producer session msg group] (type queue))))

(defmulti commit-tx (fn [queue session] (type queue)))

(defmulti create-queue (fn [queue task] (type queue)))

(defmulti bootstrap-queue (fn [queue task] (type queue)))

(defmulti close-resource (fn [queue resource] (type queue)))

(defmulti create-io-task
  (fn [element parent children phase]
    (:onyx/type element)))

