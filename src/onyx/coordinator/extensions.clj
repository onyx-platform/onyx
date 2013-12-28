(ns onyx.coordinator.extensions)

(defmulti ack (fn [log task] log))

(defmulti evict (fn [log task] log))

(defmulti complete (fn [log task] log))

(defmulti delete (fn [sync task] sync))

(defmulti cap-queue (fn [queue task] queue))

