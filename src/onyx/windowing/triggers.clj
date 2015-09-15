(ns onyx.windowing.triggers
  (:require [onyx.windowing.coerce :refer [to-standard-units]]
            [onyx.windowing.window-id :as wid]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmulti trigger-setup
  (fn [event trigger id]
    (:trigger/type trigger)))

(defmulti trigger-teardown
  (fn [event trigger id]
    (:trigger/type trigger)))

(defmulti refine-state
  (fn [event trigger]
    (:trigger/refinement trigger)))

(defmethod trigger-setup :periodically
  [event trigger id]
  ;; TODO: validate that :trigger/period is a time-based value.
  (let [f (kw->fn (:trigger/sync trigger))
        fut
        (future
          (loop []
            (try
              (let [ms (apply to-standard-units (:trigger/period trigger))]
                (Thread/sleep ms)
                (let [state (refine-state event trigger)
                      window-ids (get state (:trigger/window-id trigger))]
                  (doseq [[window-id state] window-ids]
                    (let [window (find-window (:onyx.core/windows event) (:trigger/window-id trigger))
                          win-min (or (:window/min-value window) 0)
                          w-range (apply to-standard-units (:window/range window))
                          w-slide (apply to-standard-units (:window/slide window))
                          lower (wid/extent-lower win-min w-range w-slide window-id)
                          upper (wid/extent-upper win-min w-slide window-id)]
                      (f event window-id lower upper state)))))
              (catch InterruptedException e
                (throw e))
              (catch Throwable e
                (fatal e)))
            (recur)))]
    (assoc-in event [:onyx.triggers/period-threads id] fut)))

(defmethod trigger-teardown :periodically
  [event trigger id]
  (let [fut (get-in event [:onyx.triggers/period-threads id])]
    (future-cancel fut)
    (update-in event [:onyx.triggers/period-threads] dissoc id)))

;; Adapted from Prismatic Plumbing:
;; https://github.com/Prismatic/plumbing/blob/c53ba5d0adf92ec1e25c9ab3b545434f47bc4156/src/plumbing/core.cljx#L346-L361

(defn swap-pair!
  "Like swap! but returns a pair [old-val new-val]"
  ([a f]
     (loop []
       (let [old-val @a
             new-val (f old-val)]
         (if (compare-and-set! a old-val new-val)
           [old-val new-val]
           (recur)))))
  ([a f & args]
     (swap-pair! a #(apply f % args))))

(defmethod refine-state :accumulating
  [event trigger]
  ;; Accumulating keeps the state, nothing to do here.
  @(:onyx.core/window-state event))

(defmethod refine-state :discarding
  [event trigger]
  (first (swap-pair! (:onyx.core/window-state event)
                     #(dissoc % (:trigger/window-id trigger)))))

(defmethod trigger-setup :default
  [event _ _]
  event)

(defmethod trigger-teardown :default
  [event _ _]
  event)
