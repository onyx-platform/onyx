(ns onyx.messaging.dummy-messenger
  (:require [onyx.messaging.messenger :as m]
            [onyx.messaging.atom-messenger :as am]))

(def dummy-messenger-group 
  am/atom-peer-group)

(defn dummy-messenger [opts]
  (am/atom-messenger))
