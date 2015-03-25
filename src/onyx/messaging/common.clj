(ns ^:no-doc onyx.messaging.common)

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn bind-addr [opts]
  (:onyx.messaging/bind-addr opts))

(defn external-addr [opts]
  (or (:onyx.messaging/external-addr opts)
      (bind-addr opts)))
