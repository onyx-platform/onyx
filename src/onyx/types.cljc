(ns onyx.types)

(defrecord Leaf [message flow])

(defn input [message]
  (->Leaf message nil))

(defrecord Route [flow exclusions post-transformation action])

; (defprotocol RefCounted 
;   (inc-count! [this])
;   (dec-count! [this]))

; (defrecord Ack [id completion-id ack-val ref-count timestamp]
;   RefCounted
;   (inc-count! [this]
;     (swap! ref-count inc))
;   (dec-count! [this]
;     (zero? (swap! ref-count dec))))

(defrecord Event 
  [id job-id task-id serialized-task log monitoring 
   task-information peer-opts fn replica-atom log-prefix

   ;; Task Data
   task catalog workflow flow-conditions lifecycles metadata task-map 
   windows triggers 

   ;; Task lifecycle
   lifecycle-id batch results

   ;; Task lifecycle management
   restart-ch task-kill-flag kill-flag outbox-ch group-ch 
   coordinator

   ; Derived event data
   task-type apply-fn egress-tasks ingress-tasks params batch-size

   ;; Move these to a compiled record
   ;; Compiled lifecycle functions
   #_compiled-after-ack-segment-fn 
   compiled-after-batch-fn
   compiled-after-read-batch-fn 
   compiled-after-retry-segment-fn
   compiled-after-task-fn 
   compiled-before-batch-fn
   compiled-before-task-start-fn 
   compiled-ex-fcs
   compiled-handle-exception-fn 
   compiled-norm-fcs 
   compiled-start-task-fn

   ;; Checkpointing
   slot-id messenger-slot-id

   ;; Windowing / grouping
   state grouping-fn uniqueness-task? windowed-task? uniqueness-key task-state task->group-by-fn])

(def message-id 0)
(def barrier-id 1)
(def heartbeat-id 2)
(def ready-id 3)
(def ready-reply-id 4)

(defn message [replica-version short-id payload]
  {:type message-id :replica-version replica-version :short-id short-id :payload payload})

(defn barrier [replica-version epoch short-id]
  {:type barrier-id :replica-version replica-version :epoch epoch :short-id short-id})

;; should be able to get rid of src-peer-id
(defn ready [replica-version src-peer-id short-id]
  {:type ready-id :replica-version replica-version :src-peer-id src-peer-id :short-id short-id})

(defn ready-reply [replica-version src-peer-id dst-peer-id session-id]
  {:type ready-reply-id :replica-version replica-version :src-peer-id src-peer-id 
   :dst-peer-id dst-peer-id :session-id session-id})

;; TODO SHORT ID
(defn heartbeat [replica-version epoch src-peer-id dst-peer-id session-id short-id]
  {:type heartbeat-id :replica-version replica-version :epoch epoch 
   :src-peer-id src-peer-id :dst-peer-id dst-peer-id :session-id session-id
   :short-id short-id})

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

