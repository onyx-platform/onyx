(ns onyx.messaging.short-circuit
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.concurrent.atomic AtomicLong]))

(defprotocol CommsMap
  (add [this value])
  (get-and-remove [this k]))

;; TODO, need some backpressure
;; This would be better implemented as a ring buffer, but peers may get ahead of each other 
;; which would require tracking the lowest position and cleaning only as the low watermark updates.
(defrecord Comms [^ConcurrentHashMap cmap ^AtomicLong counter]
  CommsMap
  (add [this value]
    (let [k (.incrementAndGet counter)]
      (.put cmap k value)
      k))
  (get-and-remove [this k]
    (let [v (.get cmap k)]
      (.remove cmap k)
      v)))

(defn lookup-short-circuit [short-circuit job-id replica-version session-id]
  (or (get-in @short-circuit [[job-id replica-version] session-id])
      (-> short-circuit
          (swap! update-in 
                 [[job-id replica-version] session-id]
                 (fn [s]
                   (or s (->Comms (ConcurrentHashMap.) (AtomicLong. -1)))))
          (get-in [[job-id replica-version] session-id]))))
