(ns onyx.coordinator.extensions)

(defmulti mark-peer-born (fn [log place] log))

(defmulti mark-peer-dead (fn [log place] log))

(defmulti mark-offered (fn [log] log))

(defmulti plan-job (fn [log job] log))

(defmulti ack (fn [log task] log))

(defmulti evict (fn [log task] log))

(defmulti complete (fn [log task] log))

(defmulti next-task (fn [log] log))

(defmulti create (fn [sync bucket] [sync bucket]))

(defmulti delete (fn [sync place] sync))

(defmulti write (fn [sync place contents] sync))

(defmulti read (fn [sync place] sync))

(defmulti on-change (fn [sync place cb] sync))

(defmulti cap-queue (fn [queue task] queue))

