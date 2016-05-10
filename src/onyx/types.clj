(ns onyx.types)

(defrecord Leaf [message id offset hash-group route])

(defn input 
  ([message]
   (->Leaf message nil nil nil nil))
  ([id message]
   ;; FIXME: THROWING AWAY ID, NO LONGER REQUIRED
   (->Leaf message nil nil nil nil)))

(defrecord Route [flow exclusions post-transformation action])

(defprotocol RefCounted 
  (inc-count! [this])
  (dec-count! [this]))

(defrecord Ack [id completion-id ack-val ref-count timestamp]
  RefCounted
  (inc-count! [this]
    (swap! ref-count inc))
  (dec-count! [this]
    (zero? (swap! ref-count dec))))

(defrecord Message [src-peer-id dst-task-id message])

(defrecord Barrier [src-peer-id dst-task-id replica-version epoch])

(defrecord BarrierAck [src-peer-id dst-peer-id replica-version epoch])

(defrecord Results [tree segments retries])

(defrecord Result [root leaves])

(defrecord TriggerState 
  [window-id refinement on sync fire-all-extents? state pred watermark-percentage doc 
   period threshold sync-fn id init-state create-state-update apply-state-update])

(defrecord StateEvent 
  [event-type task-event segment grouped? group-key lower-bound upper-bound 
   log-type trigger-update aggregation-update window next-state])

(defn new-state-event 
  [event-type task-event]
  (->StateEvent event-type task-event nil nil nil nil nil nil nil nil nil nil))

(defmethod clojure.core/print-method StateEvent
  [system ^java.io.Writer writer]
  (.write writer  "#<onyx.types.StateEvent>"))

(defrecord Link [link timestamp])

(defrecord MonitorEvent [event])

(defrecord MonitorEventLatency [event latency])

(defrecord MonitorEventBytes [event bytes])

(defrecord MonitorTaskEventCount [event count])

(defrecord MonitorEventLatencyBytes [event latency bytes])


