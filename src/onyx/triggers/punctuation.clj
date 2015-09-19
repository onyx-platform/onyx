(ns onyx.triggers.punctuation
  (:require [onyx.windowing.units :refer [to-standard-units standard-units-for]]
            [onyx.windowing.window-id :as wid]
            [onyx.triggers.triggers-api :as api]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.static.planning :refer [find-window]]
            [taoensso.timbre :refer [fatal]]))

(defmethod api/trigger-setup :punctuation
  [event trigger]
  (let [f (kw->fn (:trigger/pred trigger))]
    (assoc-in event [:onyx.triggers/punctuation-preds (:trigger/id trigger)] f)))

(defmethod api/trigger-notifications :punctuation
  [event trigger]
  #{:new-segment})

(defmethod api/trigger-fire? :punctuation
  [{:keys [onyx.triggers/punctuation-preds] :as event} trigger
   {:keys [window-id lower-extent upper-extent segment]}]
  ((get-in event [:onyx.triggers/punctuation-preds (:trigger/id trigger)])
   event
   window-id
   lower-extent
   upper-extent
   segment))

(defmethod api/trigger-teardown :segment
  [event trigger]
  event)
