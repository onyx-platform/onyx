(ns onyx.extensions
  "Extension interfaces for internally used queues, logs,
   and distributed coordination.")

;; Replica interface

(defmulti apply-log-entry (fn [entry replica] (:fn entry)))

(defmulti replica-diff (fn [entry old new] (:fn entry)))

(defmulti fire-side-effects! (fn [entry old new diff local-state] (:fn entry)))

(defmulti reactions (fn [entry old new diff peer-args] (:fn entry)))

;; Peer replica view interface

(defmulti new-task-state! 
  (fn [log entry old-replica new-replica diff state task-state opts] 
    :default))

;; Log interface

(defmulti write-log-entry (fn [log data] (type log)))

(defmulti read-log-entry (fn [log position] (type log)))

(defmulti register-pulse (fn [log id] (type log)))

(defmulti peer-exists? (fn [log id] (type log)))

(defmulti on-delete (fn [log id ch] (type log)))

(defmulti subscribe-to-log (fn [log ch] (type log)))

(defmulti write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti force-write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti read-chunk (fn [log kw id & args] [(type log) kw]))

(defmulti update-origin! (fn [log replica message-id] (type log)))

(defmulti gc-log-entry (fn [log position] (type log)))

;; Monitoring interface

(defmulti monitoring-agent :monitoring)

(defprotocol IEmitEvent
  (registered? [_ event-type])
  (emit [_ event]))
