(ns onyx.monitoring.custom-monitoring
  (:require [onyx.extensions :as extensions]))

(defrecord CustomMonitoringAgent
  [task
   id
   job-id
   task-id
   zookeeper-write-log-entry
   zookeeper-read-log-entry
   zookeeper-write-catalog
   zookeeper-write-workflow
   zookeeper-write-flow-conditions
   zookeeper-write-lifecycles
   zookeeper-write-task
   zookeeper-write-chunk
   zookeeper-write-job-scheduler
   zookeeper-write-messaging
   zookeeper-force-write-chunk
   zookeeper-write-origin
   zookeeper-read-catalog
   zookeeper-read-workflow
   zookeeper-read-flow-conditions
   zookeeper-read-lifecycles
   zookeeper-read-task
   zookeeper-read-chunk
   zookeeper-read-origin
   zookeeper-read-job-scheduler
   zookeeper-read-messaging
   zookeeper-gc-log-entry
   peer-ack-segments
   peer-retry-segment
   peer-try-complete-job
   peer-strip-sentinel
   peer-complete-message
   peer-gc-peer-link
   peer-backpressure-on
   peer-backpressure-off
   peer-prepare-join
   peer-notify-join
   peer-accept-join
   peer-send-bytes]
  extensions/IEmitEvent
  (extensions/registered? [this event-type]
    (get this event-type))
  (extensions/emit [this event]
    (when-let [f (get this (:event event))]
      (f this event))))

(defmethod extensions/monitoring-agent :custom
  [monitoring-config]
  (map->CustomMonitoringAgent monitoring-config))
