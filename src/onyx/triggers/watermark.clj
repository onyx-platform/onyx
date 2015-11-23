(ns onyx.triggers.watermark
  (:require [onyx.windowing.units :refer [to-standard-units coerce-key]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :watermark
  [event trigger]
  event)

(defmethod api/trigger-notifications :watermark
  [event trigger]
  #{:new-segment :task-complete})

(defn exceeds-watermark? [window upper-extent-bound segment]
  (let [watermark (get segment (:window/window-key window))]
    (>= (coerce-key watermark :milliseconds) upper-extent-bound)))

(defmethod api/trigger-fire? :watermark
  [event trigger args]
  ;; If this was stimulated by a new segment, check if it should fire.
  ;; Otherwise if this was a completed task, always fire.
  (if (:segment args)
    (exceeds-watermark? (:window args) (:upper-extent args) (:segment args))
    true))

(defmethod api/trigger-teardown :watermark
  [event trigger]
  event)
