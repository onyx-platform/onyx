(ns onyx.types)

(defrecord Route [flow exclusions post-transformation action])

(def message-id (int 0))
(def barrier-id (int 1))
(def heartbeat-id (int 2))
(def ready-id (int 3))
(def ready-reply-id (int 4))

(defn message [replica-version short-id payload]
  {:type message-id :replica-version replica-version :short-id short-id :payload payload})

(defn barrier [replica-version epoch short-id]
  {:type barrier-id :replica-version replica-version :epoch epoch :short-id short-id})

;; should be able to get rid of src-peer-id via short-id
(defn ready [replica-version src-peer-id short-id]
  {:type ready-id :replica-version replica-version :src-peer-id src-peer-id :short-id short-id})

(defn ready-reply [replica-version src-peer-id dst-peer-id session-id short-id]
  {:type ready-reply-id :replica-version replica-version :src-peer-id src-peer-id 
   :dst-peer-id dst-peer-id :session-id session-id :short-id short-id})

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

