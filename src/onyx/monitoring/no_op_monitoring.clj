(ns onyx.monitoring.no-op-monitoring
  (:require [onyx.extensions :as extensions]))

(defrecord NoOpMonitoringAgent []
  extensions/IEmitEvent
  (emit [_ event]))

(defmethod extensions/monitoring-agent :no-op
  [monitoring-config]
  (->NoOpMonitoringAgent))