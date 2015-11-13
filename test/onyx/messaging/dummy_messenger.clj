(ns onyx.messaging.dummy-messenger
  (:require [onyx.extensions :as extensions]))

(defmethod extensions/assign-acker-resources :dummy-messenger
  [config peer-id peer-site peer-sites]
  {:port 1})

(defmethod extensions/assign-task-resources :dummy-messenger
  [config peer-id task-id peer-site peer-sites]
  {:port 1})

(defmethod extensions/get-peer-site :dummy-messenger
  [replica peer]
  "localhost")

(defrecord DummyMessenger [peer-opts])

(defn dummy-messenger [peer-opts]
  (->DummyMessenger peer-opts))

(defmethod extensions/peer-site onyx.messaging.dummy_messenger.DummyMessenger
  [messenger]
  {:address 1})
