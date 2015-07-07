(ns onyx.monitoring.custom-monitoring
  (:require [onyx.extensions :as extensions]))

(def look-up-callback
  (memoize
   (fn [config callback-kw]
     (get config callback-kw (constantly nil)))))

(defrecord CustomMonitoringAgent [config]
  extensions/IEmitEvent
  (emit [_ event]
    ((look-up-callback config (:event event)) config event)))

(defmethod extensions/monitoring-agent :custom
  [monitoring-config]
  (map->CustomMonitoringAgent monitoring-config))
