(ns onyx.extensions
  "Extension interfaces for internally used queues, logs,
   and distributed coordination.")

;; Replica interface

(defmulti apply-log-entry (fn [entry replica] (:fn entry)))

(defmulti replica-diff (fn [entry old new] (:fn entry)))

(defmulti fire-side-effects! (fn [entry old new diff local-state] (:fn entry)))

(defmulti reactions (fn [entry old new diff peer-args] (:fn entry)))

;; Log interface

(defmulti write-log-entry (fn [log data] (type log)))

(defmulti read-log-entry (fn [log position] (type log)))

(defmulti register-pulse (fn [log id] (type log)))

(defmulti on-delete (fn [log id ch] (type log)))

(defmulti subscribe-to-log (fn [log ch] (type log)))

(defmulti write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti read-chunk (fn [log kw id] [(type log) kw]))

(defmulti update-origin! (fn [log replica message-id] (type log)))

(defmulti gc-log-entry (fn [log position] (type log)))

;; Messaging interface

(defmulti receive-message (fn [messaging timeout-ms] (type messaging)))

(defmulti send-message (fn [messaging message] (type messaging)))

(defmulti internal-ack-message (fn [messaging id] (type messaging)))

(defmulti internal-complete-message (fn [messaging id] (type messaging)))

;; Queue interface

(defmulti create-tx-session (fn [queue] (type queue)))

(defmulti create-consumer (fn [queue session queue-name] (type queue)))

(defmulti create-producer (fn [queue session queue-name] (type queue)))

(defmulti consume-message (fn [queue consumer] (type queue)))

(defmulti read-message (fn [queue message] (type queue)))

(defmulti message-uuid (fn [queue message] (type queue)))

(defmulti ack-message (fn [queue message] (type queue)))

(defmulti produce-message (fn ([queue producer session msg] (type queue))
                            ([queue producer session msg group] (type queue))))

(defmulti commit-tx (fn [queue session] (type queue)))

(defmulti rollback-tx (fn [queue session] (type queue)))

(defmulti create-queue (fn [queue task] (type queue)))

(defmulti create-queue-on-session (fn [queue session queue-name] (type queue)))

(defmulti n-messages-remaining (fn [queue session queue-name] (type queue)))

(defmulti n-consumers (fn [queue queue-name] (type queue)))

(defmulti optimize-concurrently (fn [queue event] (type queue)))

(defmulti bootstrap-queue (fn [queue task] (type queue)))

(defmulti close-resource (fn [queue resource] (type queue)))

(defmulti bind-active-session (fn [queue queue-name] (type queue)))

(defmulti producer->queue-name (fn [queue queue-name] (type queue)))

(defmulti create-io-task
  (fn [element parents children]
    (:onyx/type element)))

