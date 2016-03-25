(ns onyx.types)

(defrecord Event [monitoring metrics])

(defrecord Leaf [message id offset acker-id completion-id ack-val hash-group route])

(defn input 
  ([id message offset]
   (->Leaf message id offset nil nil nil nil nil))
  ([id message]
   (->Leaf message id nil nil nil nil nil nil)))

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

(defrecord Barrier [to-peer-id from-peer-id barrier-id src-task dst-task origin-peers])

(defrecord Results [tree acks segments retries])

(defrecord Result [root leaves])

(defrecord Compiled
    [bulk? compiled-after-ack-segment-fn compiled-after-batch-fn
     compiled-after-read-batch-fn compiled-after-retry-segment-fn
     compiled-after-task-fn compiled-before-batch-fn
     compiled-before-task-start-fn compiled-ex-fcs
     compiled-handle-exception-fn compiled-norm-fcs compiled-start-task-fn
     egress-ids flow-conditions fn grouping-fn id job-id messenger
     monitoring uniqueness-task? uniqueness-key peer-replica-view
     log-prefix pipeline state task->group-by-fn task-type acking-state task-information])

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


