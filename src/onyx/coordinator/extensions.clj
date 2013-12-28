(ns onyx.coordinator.extensions)

(defmulti mark-peer-born (fn [log peer] log))

(defmulti mark-peer-dead (fn [log peer] log))

(defmulti mark-offered (fn [log] log))

(defmulti plan-job (fn [log job] log))

(defmulti ack (fn [log task] log))

(defmulti evict (fn [log task] log))

(defmulti complete (fn [log task] log))

(defmulti next-task (fn [log] log))

(defmulti create (fn [sync] sync))

(defmulti delete (fn [sync task] sync))

(defmulti write (fn [sync] sync))

(defmulti read (fn [sync node] sync))

(defmulti on-change (fn [sync node cb] sync))

(defmulti cap-queue (fn [queue task] queue))

