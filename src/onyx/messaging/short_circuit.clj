(ns onyx.messaging.short-circuit
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]))

(defprotocol CommsMap
  (add [this value])
  (batch-count [this])
  (get-and-remove [this k]))

;; This would be better implemented as a ring buffer, but peers may get ahead of each other 
;; which would require tracking the lowest position and cleaning only as the low watermark updates.
(defrecord Comms [^ConcurrentHashMap cmap ^AtomicLong counter max-batches]
  CommsMap
  (batch-count [this] (.size cmap))
  (add [this value]
    (when (< (.size cmap) max-batches)
      (let [k (.incrementAndGet counter)]
        (.put cmap k value)
        k)))
  (get-and-remove [this k]
    (let [v (.get cmap k)]
      (.remove cmap k)
      v)))

(defn get-short-circuit [short-circuit job-id replica-version session-id]
  (get-in @short-circuit [[job-id replica-version] session-id]))

(defn get-init-short-circuit [short-circuit job-id replica-version session-id max-batches]
  (or (get-short-circuit short-circuit job-id replica-version session-id) 
      (-> short-circuit
          (swap! update-in 
                 [[job-id replica-version] session-id]
                 (fn [s]
                   (or s (->Comms (ConcurrentHashMap.) (AtomicLong. -1) max-batches))))
          (get-in [[job-id replica-version] session-id]))))
