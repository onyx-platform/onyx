(ns ^:no-doc onyx.messaging.common
  (:require [taoensso.timbre :refer [fatal]]))

(defn my-remote-ip []
  (apply str (butlast (slurp "http://checkip.amazonaws.com"))))

(defn bind-addr [peer-config]
  (:onyx.messaging/bind-addr peer-config))

(defn external-addr [peer-config]
  (or (:onyx.messaging/external-addr peer-config)
      (bind-addr peer-config)))

(defn aeron-channel [addr port]
  (format "aeron:udp?endpoint=%s:%s" addr port))
