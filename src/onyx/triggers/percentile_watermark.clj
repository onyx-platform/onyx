(ns onyx.triggers.percentile-watermark
  (:require [onyx.windowing.units :refer [to-standard-units coerce-key]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :percentile-watermark
  [event trigger]
  event)

(defmethod api/trigger-notifications :percentile-watermark
  [event trigger]
  #{:new-segment})

(defn exceeds-watermark? [window trigger lower-extent-bound upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))
        pct (:trigger/watermark-percentage trigger)
        offset (* (- upper-extent-bound lower-extent-bound) pct)]
    (>= (coerce-key watermark :milliseconds) (+ lower-extent-bound offset))))

(defmethod api/trigger-fire? :percentile-watermark
  [{:keys [onyx.core/window-state] :as event} trigger args]
  (exceeds-watermark? (:window args) trigger (:lower-extent args)
                      (:upper-extent args) (:segment args)))

(defmethod api/trigger-teardown :percentile-watermark
  [event trigger]
  event)
