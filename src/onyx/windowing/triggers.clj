(ns onyx.windowing.triggers
  (:require [onyx.windowing.coerce :as c]
            [onyx.peer.operation :refer [kw->fn]]
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
              (let [ms (apply c/to-standard-units (:trigger/period trigger))]
                (Thread/sleep ms)
                (let [state (refine-state event trigger)]
                  (f event state)))
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

(defn get-and-set!
  "Like reset! but returns old-val"
  [a new-val]
  (first (swap-pair! a (constantly new-val))))

(defmethod refine-state :accumulating
  [event trigger]
  ;; Accumulating keeps the state, nothing to do here.
  event)

(defmethod refine-state :discarding
  [event trigger]
  (get-and-set! (:onyx.core/window-state event) nil))
