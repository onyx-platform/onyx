(ns ^:no-doc onyx.messaging.common)

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn choose-ip [opts]
  (or (:onyx.messaging/ip opts) (my-remote-ip)))

