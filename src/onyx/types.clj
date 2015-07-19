(ns onyx.types)

(defrecord Leaf [message id acker-id completion-id ack-val ack-vals route routes hash-group])

(defn leaf 
  ([message] 
     (->Leaf message nil nil nil nil nil nil nil nil)))

(defrecord Route [flow exclusions post-transformation action])

(defrecord Ack [id completion-id ack-val timestamp])

(defrecord Result [root leaves])

(defrecord Link [link timestamp])

(defrecord MonitorEvent [event])

(defrecord MonitorEventLatency [event latency])

(defrecord MonitorEventLatencyBytes [event latency bytes])
