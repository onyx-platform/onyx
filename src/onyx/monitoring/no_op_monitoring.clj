(ns onyx.monitoring.no-op-monitoring
  (:require [onyx.extensions :as extensions]))

(defrecord NoOpMonitoringAgent []
  extensions/IEmitEvent
  (extensions/registered? [this event-type]
    false)
  (extensions/emit [_ event]))

(defn no-op-monitoring-agent []
  (->NoOpMonitoringAgent))

(defmethod extensions/monitoring-agent :no-op
  [monitoring-config]
  (->NoOpMonitoringAgent))
