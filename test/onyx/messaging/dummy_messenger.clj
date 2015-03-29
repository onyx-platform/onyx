(ns onyx.messaging.dummy-messenger
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/assign-site-resources :dummy-messenger
  [config peer-site peer-sites]
  {:port 1})

(defrecord DummyMessenger [])

(defmethod extensions/peer-site onyx.messaging.dummy_messenger.DummyMessenger
  [messenger]
  {:address 1})
