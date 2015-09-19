(ns onyx.triggers.segment
  (:require [onyx.windowing.units :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :segment
  [event trigger]
  (if (= (standard-units-for (second (:trigger/threshold trigger))) :elements)
    (assoc-in event [:onyx.triggers/segments] (atom {}))
    (throw (ex-info ":trigger/threshold must be a unit that can be converted to :elements" {:trigger trigger}))))

(defmethod api/trigger-notifications :segment
  [event trigger]
  #{:new-segment})

(defmethod api/trigger-fire? :segment
  [{:keys [onyx.core/window-state] :as event} trigger {:keys [segment]}]
  (let [id (:trigger/id trigger)
        segment-state @(:onyx.triggers/segments event)
        x ((fnil inc 0) (get segment-state id))
        fire? (>= x (apply to-standard-units (:trigger/threshold trigger)))]
    (if fire?
      (swap! (:onyx.triggers/segments event) dissoc id)
      (swap! (:onyx.triggers/segments event) update-in [id] (fnil inc 0)))
    fire?))

(defmethod api/trigger-teardown :segment
  [event trigger]
  (dissoc event :onyx.triggers/segments))
