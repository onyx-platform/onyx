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
                (f event @(:onyx.core/window-state event)))
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
