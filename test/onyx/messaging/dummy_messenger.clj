(ns onyx.messaging.dummy-messenger
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/assign-site-resources :dummy-messenger
  [config peer-site peer-sites]
  {:port 1})

(defrecord DummyMessenger [peer-opts])

(defn dummy-messenger [peer-opts]
  (->DummyMessenger peer-opts))

(defmethod extensions/peer-site onyx.messaging.dummy_messenger.DummyMessenger
  [messenger]
  {:address 1})

(defmethod extensions/open-peer-site :dummy-messenger
  [_ _] "localhost")
