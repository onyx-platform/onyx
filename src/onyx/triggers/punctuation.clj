(ns onyx.triggers.punctuation
  (:require [onyx.windowing.coerce :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :punctuation
  [event trigger id]
  (let [f (kw->fn (:trigger/pred trigger))]
    (assoc-in event [:onyx.triggers/punctuation-preds id] f)))

(defmethod api/trigger-notifications :punctuation
  [event trigger id]
  #{:new-segment})

(defmethod api/trigger-fire :punctuation
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
        (when ((get-in event [:onyx.triggers/punctuation-preds id])
               event
               window-id
               lower
               upper
               segment)
          (api/refine-state event trigger)
          (f event window-id lower upper state))))))

(defmethod api/trigger-teardown :segment
  [event trigger id]
  event)
