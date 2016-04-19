(ns onyx.messaging.dummy-messenger
  (:require [onyx.messaging.messenger :as m]))

(defmethod m/assign-task-resources :dummy-messenger
  [config peer-id task-id peer-site peer-sites]
  {:port 1})

(defmethod m/get-peer-site :dummy-messenger
  [replica peer]
  "localhost")

(defrecord DummyMessengerGroup [peer-opts]
  m/MessengerGroup
  (peer-site [messenger-group peer-id]
    {}))

(defn dummy-messenger-group [peer-opts]
  (->DummyMessengerGroup peer-opts))


