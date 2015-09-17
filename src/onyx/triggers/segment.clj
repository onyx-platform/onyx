(ns onyx.triggers.segment
  (:require [onyx.windowing.coerce :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :segment
  [event trigger id]
  (if (= (standard-units-for (second (:trigger/threshold trigger))) :elements)
    (assoc-in event [:onyx.triggers/segments] (atom {}))
    (throw (ex-info ":trigger/threshold must be a unit that can be converted to :elements" {:trigger trigger}))))

(defmethod api/trigger-notifications :segment
  [event trigger id]
  #{:new-segment})

(defmethod api/trigger-fire :segment
  [{:keys [onyx.core/window-state] :as event} trigger id]
  (let [segment-state @(:onyx.triggers/segments event)
        f (kw->fn (:trigger/sync trigger))
        x ((fnil inc 0) (get segment-state id))]
    (if (>= x (apply to-standard-units (:trigger/threshold trigger)))
      (let [state (api/refine-state event trigger)
            window-ids (get state (:trigger/window-id trigger))]
        (doseq [[window-id state] window-ids]
          (let [window (find-window (:onyx.core/windows event) (:trigger/window-id trigger))
                win-min (or (:window/min-value window) 0)
                w-range (apply to-standard-units (:window/range window))
                w-slide (apply to-standard-units (or (:window/slide window) (:window/range window)))
                lower (wid/extent-lower win-min w-range w-slide window-id)
                upper (wid/extent-upper win-min w-slide window-id)]
            (f event window-id lower upper state)))
        (swap! (:onyx.triggers/segments event) dissoc id))
      (swap! (:onyx.triggers/segments event) update-in [id] (fnil inc 0)))))

(defmethod api/trigger-teardown :segment
  [event trigger id]
  (dissoc event :onyx.triggers/segments))
