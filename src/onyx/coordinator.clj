(ns onyx.coordinator
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/connect :mem
  [uri])

(defmethod extensions/connect :netty
  [uri])

