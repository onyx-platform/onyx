(ns onyx.messaging.dummy-messenger
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/assign-site-resources :dummy-messenger
  [config peer-site peer-sites]
  {:port 1})

(defmethod extensions/peer-site clojure.lang.Keyword
  [messenger]
  {:address 1})
