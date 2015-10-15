(ns onyx.triggers.timer
  (:require [onyx.windowing.units :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :timer
  [{:keys [onyx.core/window-state] :as event} trigger]
  (if (= (standard-units-for (second (:trigger/period trigger))) :milliseconds)
    (let [ms (apply to-standard-units (:trigger/period trigger))
          fut
          (future
            (loop []
              (try
                (Thread/sleep ms)
                (api/fire-trigger! event window-state trigger {:context :timer})
                (catch InterruptedException e
                  (throw e))
                (catch Throwable e
                  (fatal e)))
              (recur)))]
      (assoc-in event [:onyx.triggers/period-threads (:trigger/id trigger)] fut))
    (throw (ex-info ":trigger/period must be a unit that can be converted to :milliseconds" {:trigger trigger}))))

(defmethod api/trigger-notifications :timer
  [event trigger]
  #{:timer})

(defmethod api/trigger-fire? :timer
  [event trigger args]
  true)

(defmethod api/trigger-teardown :timer
  [event {:keys [trigger/id] :as trigger}]
  (let [fut (get-in event [:onyx.triggers/period-threads id])]
    (future-cancel fut)
    (update-in event [:onyx.triggers/period-threads] dissoc id)))
