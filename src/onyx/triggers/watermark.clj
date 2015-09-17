(ns onyx.triggers.watermark
  (:require [onyx.windowing.coerce :refer [to-standard-units coerce-key]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :watermark
  [event trigger id]
  event)

(defmethod api/trigger-notifications :watermark
  [event trigger id]
  #{:new-segment})

(defn exceeds-watermark? [window upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))]
    (>= (coerce-key watermark :milliseconds) upper-extent-bound)))

(defmethod api/trigger-fire :watermark
  [{:keys [onyx.core/window-state] :as event} trigger id segment]
  (let [f (kw->fn (:trigger/sync trigger))
        window-ids (get @window-state (:trigger/window-id trigger))]
    (doseq [[window-id state] window-ids]
      (let [window (find-window (:onyx.core/windows event) (:trigger/window-id trigger))
            win-min (or (:window/min-value window) 0)
            w-range (apply to-standard-units (:window/range window))
            w-slide (apply to-standard-units (or (:window/slide window) (:window/range window)))
            lower (wid/extent-lower win-min w-range w-slide window-id)
            upper (wid/extent-upper win-min w-slide window-id)]
        (when (exceeds-watermark? window upper segment)
          (api/refine-state event trigger)
          (f event window-id lower upper state))))))

(defmethod api/trigger-teardown :segment
  [event trigger id]
  event)
