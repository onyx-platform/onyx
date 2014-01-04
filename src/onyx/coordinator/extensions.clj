(ns onyx.coordinator.extensions)

(defmulti mark-peer-born (fn [log place] (type log)))

(defmulti mark-peer-dead (fn [log place] (type log)))

(defmulti mark-offered (fn [log] (type log)))

(defmulti plan-job (fn [log tasks] (type log)))

(defmulti ack (fn [log task] (type log)))

(defmulti evict (fn [log task] (type log)))

(defmulti complete (fn [log task] (type log)))

(defmulti next-task (fn [log] (type log)))

(defmulti create (fn [sync bucket] [(type sync) bucket]))

(defmulti delete (fn [sync place] (type sync)))

(defmulti write-place (fn [sync place contents] (type sync)))

(defmulti read-place (fn [sync place] (type sync)))

(defmulti on-change (fn [sync place cb] (type sync)))

(defmulti cap-queue (fn [queue task] queue))

(defmulti create-io-task
  (fn [element parent children]
    (select-keys element [:onyx/direction :onyx/type :onyx/medium])))

