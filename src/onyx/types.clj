(ns onyx.types)

(defrecord Event [monitoring metrics])

(defrecord Leaf [message id acker-id completion-id ack-val hash-group route])

(defn input [id message]
  (->Leaf message id nil nil nil nil nil))

(defrecord Route [flow exclusions post-transformation action])

(defprotocol RefCounted 
  (inc-count! [this])
  (dec-count! [this]))

(defrecord Ack [id completion-id ack-val ref-count timestamp]
  RefCounted
  (inc-count! [this]
    (swap! (:ref-count this) inc))
  (dec-count! [this]
    (zero? (swap! (:ref-count this) dec))))

(defrecord Results [tree acks segments retries])

(defrecord Result [root leaves])

(defrecord Compiled [bulk? compiled-after-ack-segment-fn compiled-after-batch-fn compiled-after-read-batch-fn 
                     compiled-after-retry-segment-fn compiled-after-task-fn compiled-before-batch-fn compiled-before-task-start-fn 
                     compiled-ex-fcs compiled-handle-exception-fn compiled-norm-fcs compiled-start-task-fn 
                     egress-ids flow-conditions fn grouping-fn id job-id messenger monitoring 
                     peer-replica-view pipeline state task->group-by-fn task-type])

(defrecord Link [link timestamp])

(defrecord MonitorEvent [event])

(defrecord MonitorEventLatency [event latency])

(defrecord MonitorEventBytes [event bytes])

(defrecord MonitorTaskEventCount [event count])

(defrecord MonitorEventLatencyBytes [event latency bytes])
