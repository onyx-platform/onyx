(ns onyx.monitoring.custom-monitoring
  (:require [onyx.extensions :as extensions]))

(def look-up-callback
  (memoize
   (fn [config callback-kw]
     (get config callback-kw (constantly nil)))))

(defrecord CustomMonitoringAgent [monitoring-config]
  extensions/IEmitEvent
  (extensions/emit [_ event]
    ((look-up-callback monitoring-config (:event event)) monitoring-config event)))

(defmethod extensions/monitoring-agent :custom
  [monitoring-config]
  (->CustomMonitoringAgent monitoring-config))
