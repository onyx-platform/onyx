(ns onyx.extensions
  "Extension interfaces for internally used queues, logs,
   and distributed coordination.")

(defmulti mark-peer-born (fn [sync node] (type sync)))

(defmulti mark-peer-dead (fn [sync node] (type sync)))

(defmulti mark-offered (fn [sync task peer nodes] (type sync)))

(defmulti plan-job (fn [sync job-id tasks catalog workflow] (type sync)))

(defmulti ack (fn [sync node] (type sync)))

(defmulti seal-resource? (fn [sync node] (type sync)))

(defmulti revoke-offer (fn [sync ack-node] (type sync)))

(defmulti complete (fn [sync complete-node cooldown-node cb] (type sync)))

(defmulti next-tasks (fn [sync] (type sync)))

(defmulti idle-peers (fn [sync] (type sync)))

(defmulti create
  (fn
    ([sync bucket] [(type sync) bucket])
    ([sync bucket content] [(type sync) bucket])))

(defmulti create-at
  (fn
    ([sync bucket subpath] [(type sync) bucket])
    ([sync bucket subpath content] [(type sync) bucket])))

(defmulti create-node (fn [sync node] (type sync)))

(defmulti delete (fn [sync node] (type sync)))

(defmulti write-node (fn [sync node contents] (type sync)))

(defmulti touch-node (fn [sync node] (type sync)))

(defmulti touched? (fn [sync bucket node] [(type sync) bucket]))

(defmulti read-node (fn [sync node] (type sync)))

(defmulti read-node-at (fn [sync node & subpaths] [(type sync) node]))

(defmulti list-nodes (fn [sync bucket] [(type sync) bucket]))

(defmulti dereference (fn [sync node] (type sync)))

(defmulti previous-node (fn [sync node] (type sync)))

(defmulti smallest? (fn [sync bucket node] [(type sync) bucket]))

(defmulti leader (fn [sync bucket node] [(type sync) bucket]))

(defmulti resolve-node (fn [sync bucket & subpath] [(type sync) bucket]))

(defmulti children (fn [sync node] (type sync)))

(defmulti node-exists? (fn [sync node] (type sync)))

(defmulti node-exists-at? (fn [sync bucket & subpaths] [(type sync) bucket]))

(defmulti creation-time (fn [sync node] (type sync)))

(defmulti bucket (fn [sync bucket] [(type sync) bucket]))

(defmulti bucket-at (fn [sync bucket subpath] [(type sync) bucket]))

(defmulti version (fn [sync node] (type sync)))

(defmulti on-change (fn [sync node cb] (type sync)))

(defmulti on-child-change (fn [sync node cb] (type sync)))

(defmulti on-delete (fn [sync node db] (type sync)))

(defmulti next-offset (fn [sync bucket] [(type sync) bucket]))

(defmulti log-entry-at (fn [sync bucket offset] [(type sync) bucket]))

(defmulti checkpoint (fn [sync bucket offset] [(type sync) bucket]))

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

(defmulti create-queue-on-session (fn [queue session queue-name] (type queue)))

(defmulti n-messages-remaining (fn [queue session queue-name] (type queue)))

(defmulti n-consumers (fn [queue queue-name] (type queue)))

(defmulti optimize-concurrently (fn [queue event] (type queue)))

(defmulti bootstrap-queue (fn [queue task] (type queue)))

(defmulti close-resource (fn [queue resource] (type queue)))

(defmulti bind-active-session (fn [queue queue-name] (type queue)))

(defmulti create-io-task
  (fn [element parent children phase]
    (:onyx/type element)))

