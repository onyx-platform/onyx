(ns onyx.triggers.timer
  (:require [onyx.windowing.coerce :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :timer
  [event trigger id]
  (if (= (standard-units-for (second (:trigger/period trigger))) :milliseconds)
    (let [ms (apply to-standard-units (:trigger/period trigger))
          fut
          (future
            (loop []
              (try
                (Thread/sleep ms)
                (api/trigger-fire event trigger id)
                (catch InterruptedException e
                  (throw e))
                (catch Throwable e
                  (fatal e)))
              (recur)))]
      (assoc-in event [:onyx.triggers/period-threads id] fut))
    (throw (ex-info ":trigger/period must be a unit that can be converted to :milliseconds" {:trigger trigger}))))

(defmethod api/trigger-notifications :timer
  [event trigger id]
  #{})

(defmethod api/trigger-fire :timer
  [event trigger id]
  (let [f (kw->fn (:trigger/sync trigger))
        state (api/refine-state event trigger)
        window-ids (get state (:trigger/window-id trigger))]
    (doseq [[window-id state] window-ids]
      (let [window (find-window (:onyx.core/windows event) (:trigger/window-id trigger))
            win-min (or (:window/min-value window) 0)
            w-range (apply to-standard-units (:window/range window))
            w-slide (apply to-standard-units (or (:window/slide window) (:window/range window)))
            lower (wid/extent-lower win-min w-range w-slide window-id)
            upper (wid/extent-upper win-min w-slide window-id)]
        (f event window-id lower upper state)))))

(defmethod api/trigger-teardown :timer
  [event trigger id]
  (let [fut (get-in event [:onyx.triggers/period-threads id])]
    (future-cancel fut)
    (update-in event [:onyx.triggers/period-threads] dissoc id)))
