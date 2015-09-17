(ns onyx.triggers.triggers-api
  (:require [taoensso.timbre :refer [info warn fatal]]))

(defmulti trigger-setup
  (fn [event trigger id]
    (:trigger/on trigger)))

(defmulti trigger-notifications
  (fn [event trigger id]
    (:trigger/on trigger)))

(defmulti trigger-fire
  (fn [event trigger id & args]
    (:trigger/on trigger)))

(defmulti trigger-teardown
  (fn [event trigger id]
    (:trigger/on trigger)))

(defmulti refine-state
  (fn [event trigger]
    (:trigger/refinement trigger)))

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
  [{:keys [onyx.core/window-state]} trigger]
  @window-state)

(defmethod refine-state :discarding
  [{:keys [onyx.core/window-state]} trigger]
  (first (swap-pair! window-state #(dissoc % (:trigger/window-id trigger)))))

(defmethod trigger-setup :default
  [event trigger id]
  event)

(defmethod trigger-fire :default
  [event trigger id invoker])

(defmethod trigger-teardown :default
  [event trigger id]
  event)
