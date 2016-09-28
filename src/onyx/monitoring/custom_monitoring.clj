(ns onyx.monitoring.custom-monitoring
  (:require [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [info warn trace fatal error] :as timbre]))

(defrecord CustomMonitoringAgent
  [task-information
   zookeeper-write-log-entry
   zookeeper-read-log-entry
   zookeeper-write-catalog
   zookeeper-write-workflow
   zookeeper-write-flow-conditions
   zookeeper-write-lifecycles
   zookeeper-write-windows
   zookeeper-write-triggers
   zookeeper-write-job-metadata
   zookeeper-write-task
   zookeeper-write-chunk
   zookeeper-write-log-parameters
   zookeeper-write-exception
   zookeeper-force-write-chunk
   zookeeper-write-origin
   zookeeper-read-catalog
   zookeeper-read-workflow
   zookeeper-read-flow-conditions
   zookeeper-read-lifecycles
   zookeeper-read-windows
   zookeeper-read-triggers
   zookeeper-read-job-metadata
   zookeeper-read-task
   zookeeper-read-chunk
   zookeeper-read-origin
   zookeeper-read-log-parameters
   zookeeper-read-exception
   zookeeper-gc-log-entry
   window-log-write-entry
   window-log-playback
   window-log-compaction
   messenger-queue-count
   messenger-queue-count-unregister
   messenger-queue-wait
   monitoring-config
   peer-processed-segments
   peer-batch-latency
   peer-ack-segments
   peer-ack-segment
   peer-complete-segment
   peer-retry-segment
   peer-try-complete-job
   peer-strip-sentinel
   peer-sentinel-found
   peer-gc-peer-link
   peer-backpressure-on
   peer-backpressure-off
   group-prepare-join
   group-notify-join
   group-accept-join
   peer-send-bytes]
  extensions/IEmitEvent
  (extensions/registered? [this event-type]
    (get this event-type))
  (extensions/emit [this event]
    (when-let [f (get this (:event event))]
      (f this event)))
  component/Lifecycle
  (start [component]
    component)
  (stop [component] 
    component))

(defmethod extensions/monitoring-agent :custom
  [monitoring-config]
  (map->CustomMonitoringAgent monitoring-config))
