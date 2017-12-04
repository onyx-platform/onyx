(ns onyx.extensions
  "Extension interfaces for internally used queues, logs,
   and distributed coordination.")

;; Replica interface

(defmulti apply-log-entry (fn [entry replica] (:fn entry)))

(defmulti replica-diff (fn [entry old new] (:fn entry)))

(defmulti fire-side-effects! 
  (fn [entry old new diff state] 
    (assert (:fn entry))
    (assert (#{:client :peer :group} (:type state)))
    [(:fn entry) (:type state)]))

(defmulti reactions 
  (fn [entry old new diff state] 
    (assert (:fn entry))
    (assert (#{:client :peer :group} (:type state)))
    [(:fn entry) (:type state)]))

(defmethod reactions :default 
  [_ _ _ _ _]
  [])

(defmethod fire-side-effects! :default 
  [_ old new _ state]
  (assert (or (not= (:type state) :peer)
              (= (:allocations old)
                 (:allocations new)))
          "fire-side-effects! fell through to default erroneously when peers should reallocate.")
  state)

;; Peer replica view interface

(defmulti new-task-state! 
  (fn [log entry old-replica new-replica diff state task-state opts] 
    :default))

;; Log interface

(defmulti write-log-entry (fn [log data] (type log)))

(defmulti read-log-entry (fn [log position] (type log)))

(defmulti register-pulse (fn [log id] (type log)))

(defmulti group-exists? (fn [log id] (type log)))

(defmulti on-delete (fn [log id ch] (type log)))

(defmulti subscribe-to-log (fn [log ch] (type log)))

(defmulti job-status (fn [log tenancy-id job-id] (type log)))

(defmulti write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti force-write-chunk (fn [log kw chunk id] [(type log) kw]))

(defmulti read-chunk (fn [log kw id & args] [(type log) kw]))

(defmulti write-job-name-metadata (fn [log chunk job-name] [(type log)]))

(defmulti read-job-name-metadata (fn [log job-name position] [(type log)]))

(defmulti update-origin! (fn [log replica message-id] (type log)))

(defmulti gc-log-entry (fn [log position] (type log)))

(defmulti connected? (fn [log] (type log)))

;; Monitoring interface

(defmulti monitoring-agent :monitoring)

(defprotocol IEmitEvent
  (registered? [_ event-type])
  (emit [_ event]))
