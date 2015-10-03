(ns onyx.state.state-extensions)

(defmulti initialise-log 
  (fn [log-type event]
    log-type))

(defmulti close-log 
  (fn [log]
    (type log)))

;; For onyx to implement
;; We can implement log storage for Kafka and maybe ZK (if small data is used and we gc)
(defmulti store-log-entry 
  "Store state update [op k v] entries in a log"
  (fn [log event entry]
    (type log)))

(defmulti playback-log-entries 
  "Read state update [op k v] entries from log"
  (fn [log event state]
    (type log)))

(defmulti store-seen-ids 
  "Store seen ids in a log. Ideally these will be timestamped for auto expiry"
  (fn [log event seen-ids]
    (type log)))

(defmulti playback-seen-ids 
  "Read seen ids from a log"
  (fn [log event bucket-state apply-fn]
    (type log)))
