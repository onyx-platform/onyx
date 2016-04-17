(ns onyx.messaging.dummy-messenger
  (:require [onyx.messaging.messenger :as m]))

(defmethod m/assign-task-resources :dummy-messenger
  [config peer-id task-id peer-site peer-sites]
  {:port 1})

(defmethod m/get-peer-site :dummy-messenger
  [replica peer]
  "localhost")

(defrecord DummyMessenger [peer-opts]
  m/Messenger
  (peer-site [messenger]
    {:address 1}))

(defn dummy-messenger [peer-opts]
  (->DummyMessenger peer-opts))


