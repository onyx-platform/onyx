(ns onyx.types)

(defrecord Route [flow exclusions post-transformation action pred-failure])

(def message-id ^:const (byte 0))
(def barrier-id ^:const (byte 1))
(def heartbeat-id ^:const (byte 2))
(def ready-id ^:const (byte 3))
(def ready-reply-id ^:const (byte 4))

(def max-control-message-size 500)

(defn message [replica-version short-id payload]
  {:type message-id :replica-version replica-version :short-id short-id :payload payload})

(defn barrier [replica-version epoch short-id]
  {:type barrier-id :replica-version replica-version :epoch epoch :short-id short-id})

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
  {:event-type event-type :task-event task-event}
  #_(->StateEvent event-type task-event nil nil nil nil nil nil nil nil nil nil))

#?(:clj (defmethod clojure.core/print-method StateEvent
          [system ^java.io.Writer writer]
          (.write writer  "#<onyx.types.StateEvent>")))

(defrecord Link [link timestamp])

(defrecord MonitorEvent [event])

(defrecord MonitorEventLatency [event latency])

(defrecord MonitorEventBytes [event bytes])

(defrecord MonitorTaskEventCount [event count])

(defrecord MonitorEventLatencyBytes [event latency bytes])

