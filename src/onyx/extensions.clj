(ns onyx.extensions)

(defmulti mark-peer-born (fn [log place] (type log)))

(defmulti mark-peer-dead (fn [log place] (type log)))

(defmulti mark-offered (fn [log task peer nodes] (type log)))

(defmulti plan-job (fn [log catalog workflow tasks] (type log)))

(defmulti ack (fn [log place] (type log)))

(defmulti seal-resource? (fn [log place] (type log)))

(defmulti revoke-offer (fn [log ack-place] (type log)))

(defmulti complete (fn [log complete-place] (type log)))

(defmulti next-tasks (fn [log] (type log)))

(defmulti nodes (fn [log peer] (type log)))

(defmulti node-basis (fn [log basis node] (type log)))

(defmulti node->task (fn [log basis node] (type log)))

(defmulti idle-peers (fn [log] (type log)))

(defmulti create (fn [sync bucket] [(type sync) bucket]))

(defmulti delete (fn [sync place] (type sync)))

(defmulti write-place (fn [sync place contents] (type sync)))

(defmulti touch-place (fn [sync place] (type sync)))

(defmulti read-place (fn [sync place] (type sync)))

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

(defmulti produce-message (fn [queue producer session msg] (type queue)))

(defmulti commit-tx (fn [queue session] (type queue)))

(defmulti create-queue (fn [queue task] (type queue)))

(defmulti bootstrap-queue (fn [queue task] (type queue)))

(defmulti close-resource (fn [queue resource] (type queue)))

(defmulti create-io-task
  (fn [element parent children phase]
    (:onyx/direction element)))

