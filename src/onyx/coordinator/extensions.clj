(ns onyx.coordinator.extensions)

(defmulti mark-peer-born (fn [log peer] log))

(defmulti mark-peer-dead (fn [log peer] log))

(defmulti mark-offered (fn [log] log))

(defmulti plan-job (fn [log job] log))

(defmulti ack (fn [log task] log))

(defmulti evict (fn [log task] log))

(defmulti complete (fn [log task] log))

(defmulti next-task (fn [log] log))

(defmulti delete (fn [sync task] sync))

(defmulti create-ack-node (fn [sync] sync))

(defmulti create-completion-node (fn [sync] sync))

(defmulti await-ack (fn [sync node] sync))

(defmulti await-completion (fn [sync node] sync))

(defmulti await-death (fn [sync peer] sync))

(defmulti write-payload (fn [sync] sync))

(defmulti cap-queue (fn [queue task] queue))

