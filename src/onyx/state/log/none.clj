(ns onyx.state.log.none
  (:require [onyx.state.state-extensions :as state-extensions]))

(defrecord None [])

(defmethod state-extensions/initialize-log :none [log-type event] 
  (->None))

(defmethod state-extensions/playback-log-entries onyx.state.log.none.None
  [none event state]
  state)

(defmethod state-extensions/close-log onyx.state.log.none.None
  [none event])

(defmethod state-extensions/store-log-entry onyx.state.log.none.None
  [none event ack-fn entry]
  (ack-fn))
