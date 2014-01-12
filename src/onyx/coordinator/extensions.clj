(ns onyx.coordinator.extensions)

(defmulti mark-peer-born (fn [log place] (type log)))

(defmulti mark-peer-dead (fn [log place] (type log)))

(defmulti mark-offered (fn [log task peer nodes] (type log)))

(defmulti plan-job (fn [log catalog workflow tasks] (type log)))

(defmulti ack (fn [log place] (type log)))

(defmulti evict (fn [log peer] (type log)))

(defmulti complete (fn [log complete-place] (type log)))

(defmulti next-task (fn [log] (type log)))

(defmulti nodes (fn [log peer] (type log)))

(defmulti idle-peer (fn [log] (type log)))

(defmulti create (fn [sync bucket] [(type sync) bucket]))

(defmulti delete (fn [sync place] (type sync)))

(defmulti write-place (fn [sync place contents] (type sync)))

(defmulti touch-place (fn [sync place] (type sync)))

(defmulti read-place (fn [sync place] (type sync)))

(defmulti on-change (fn [sync place cb] (type sync)))

(defmulti on-delete (fn [sync place db] (type sync)))

(defmulti cap-queue (fn [queue task] queue))

(defmulti create-io-task
  (fn [element parent children phase]
    (select-keys element [:onyx/direction :onyx/type :onyx/medium])))

